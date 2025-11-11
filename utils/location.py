"""
Location utilities: Distance calculations and GPS geolocation
"""
import math
import requests
from datetime import datetime, timedelta
from typing import Tuple, Optional, Dict, List


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate distance between two coordinates using Haversine formula.
    Returns distance in kilometers.
    """
    try:
        lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])
    except (TypeError, ValueError):
        return None

    R = 6371  # Earth's radius in km

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = math.sin(dlat/2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def get_location_info(latitude: float, longitude: float) -> Optional[Dict]:
    """
    Get country and city information from coordinates using OpenStreetMap Nominatim API.
    
    Args:
        latitude: Location latitude
        longitude: Location longitude
        
    Returns:
        Dictionary with country, city and other location info or None if error
    """
    try:
        # Nominatim API for reverse geocoding (free, no API key required)
        url = "https://nominatim.openstreetmap.org/reverse"
        params = {
            "lat": latitude,
            "lon": longitude,
            "format": "json",
            "accept-language": "pt-BR,pt,en",
            "zoom": 10  # City level detail
        }
        
        headers = {
            "User-Agent": "VivaAI-App/1.0"  # Required by Nominatim
        }

        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()

        data = response.json()
        
        if "address" not in data:
            return None
            
        address = data["address"]
        
        # Extract relevant location information
        country = address.get("country")
        country_code = address.get("country_code", "").upper()
        
        # Try different city field names
        city = (address.get("city") or 
                address.get("town") or 
                address.get("village") or 
                address.get("municipality") or 
                address.get("state_district"))
        
        state = address.get("state")
        region = address.get("region")
        
        return {
            "country": country,
            "country_code": country_code,
            "city": city,
            "state": state,
            "region": region,
            "display_name": data.get("display_name"),
            "latitude": latitude,
            "longitude": longitude,
            "source": "nominatim"
        }

    except requests.exceptions.Timeout:
        print(f"[WARNING] Timeout fetching location info for {latitude},{longitude}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"[WARNING] Error fetching location info: {str(e)}")
        return None
    except Exception as e:
        print(f"[WARNING] Unexpected error in get_location_info: {str(e)}")
        return None


def get_historical_temperature(
    latitude: float,
    longitude: float,
    target_date: datetime,
    target_hour: Optional[int] = None
) -> Optional[Dict]:
    """
    Get historical temperature from Open-Meteo API.

    Args:
        latitude: Location latitude
        longitude: Location longitude
        target_date: Date to fetch temperature for
        target_hour: Specific hour (0-23), if None returns all day data

    Returns:
        Dictionary with temperature data or None if error
    """
    try:
        # Format date for API
        date_str = target_date.strftime("%Y-%m-%d")

        # Open-Meteo API for historical data
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": date_str,
            "end_date": date_str,
            "hourly": "temperature_2m",
            "timezone": "auto"
        }

        response = requests.get(url, params=params, timeout=5)
        response.raise_for_status()

        data = response.json()

        if "hourly" not in data or "temperature_2m" not in data["hourly"]:
            return None

        temps = data["hourly"]["temperature_2m"]

        if target_hour is not None and 0 <= target_hour < len(temps):
            # Return specific hour temperature
            return {
                "temperature_c": temps[target_hour],
                "temperature_f": (temps[target_hour] * 9/5) + 32,
                "date": date_str,
                "hour": target_hour,
                "source": "open-meteo"
            }
        else:
            # Return all day data
            avg_temp = sum(temps) / len(temps) if temps else None
            max_temp = max(temps) if temps else None
            min_temp = min(temps) if temps else None

            return {
                "temperature_c_avg": avg_temp,
                "temperature_c_max": max_temp,
                "temperature_c_min": min_temp,
                "temperature_f_avg": (avg_temp * 9/5) + 32 if avg_temp else None,
                "temperature_f_max": (max_temp * 9/5) + 32 if max_temp else None,
                "temperature_f_min": (min_temp * 9/5) + 32 if min_temp else None,
                "hourly_data": temps,
                "date": date_str,
                "source": "open-meteo"
            }

    except requests.exceptions.Timeout:
        print(f"[WARNING] Timeout fetching temperature for {latitude},{longitude}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"[WARNING] Error fetching temperature: {str(e)}")
        return None
    except Exception as e:
        print(f"[WARNING] Unexpected error in get_historical_temperature: {str(e)}")
        return None


def correlate_asset_temperature_with_location(
    asset_temperature_c: float,
    asset_latitude: float,
    asset_longitude: float,
    event_time: datetime,
    location_latitude: float,
    location_longitude: float
) -> Optional[Dict]:
    """
    Get location temperature for the same time the asset temperature was recorded.

    Returns dict with comparison data.
    """
    try:
        # Get temperature at location for same date/hour
        location_temp_data = get_historical_temperature(
            location_latitude,
            location_longitude,
            event_time,
            event_time.hour
        )

        if not location_temp_data:
            return None

        location_temp_c = location_temp_data.get("temperature_c")

        if location_temp_c is None:
            return None

        # Calculate difference
        temp_diff = asset_temperature_c - location_temp_c

        return {
            "asset_temperature_c": asset_temperature_c,
            "location_temperature_c": location_temp_c,
            "temperature_difference_c": temp_diff,
            "temperature_difference_f": temp_diff * 9/5,
            "asset_warmer": temp_diff > 0,
            "temperature_ratio": asset_temperature_c / location_temp_c if location_temp_c != 0 else None,
            "location_data": location_temp_data
        }

    except Exception as e:
        print(f"[ERROR] Error correlating temperatures: {str(e)}")
        return None


def sort_outlets_by_distance(
    user_latitude: float,
    user_longitude: float,
    outlets: List[Dict]
) -> List[Dict]:
    """
    Sort outlets by distance from user location.
    Adds 'distance_km' field to each outlet.
    """
    try:
        user_lat, user_lon = float(user_latitude), float(user_longitude)
    except (TypeError, ValueError):
        return outlets

    outlets_with_distance = []

    for outlet in outlets:
        try:
            outlet_lat = outlet.get("latitude")
            outlet_lon = outlet.get("longitude")

            if outlet_lat and outlet_lon:
                distance = haversine_distance(user_lat, user_lon, outlet_lat, outlet_lon)
                outlet_copy = dict(outlet)
                outlet_copy["distance_km"] = round(distance, 2) if distance else None
                outlets_with_distance.append(outlet_copy)
            else:
                # Keep outlets without coordinates at the end
                outlet_copy = dict(outlet)
                outlet_copy["distance_km"] = None
                outlets_with_distance.append(outlet_copy)

        except Exception as e:
            print(f"[WARNING] Error processing outlet {outlet.get('code')}: {str(e)}")
            continue

    # Sort by distance (None values go to end)
    return sorted(
        outlets_with_distance,
        key=lambda x: (x["distance_km"] is None, x["distance_km"] if x["distance_km"] else float('inf'))
    )


def filter_outlets_by_location(
    user_latitude: float,
    user_longitude: float,
    outlets: List[Dict],
    user_location_info: Optional[Dict] = None
) -> List[Dict]:
    """
    Filter outlets by location (same country/city as user) and then sort by distance.
    
    Args:
        user_latitude: User's latitude
        user_longitude: User's longitude  
        outlets: List of outlet dictionaries
        user_location_info: Optional pre-fetched location info to avoid API call
        
    Returns:
        Filtered and sorted list of outlets
    """
    try:
        # Get user's location info if not provided
        if not user_location_info:
            user_location_info = get_location_info(user_latitude, user_longitude)
        
        if not user_location_info:
            print("[WARNING] Could not get user location info, returning all outlets sorted by distance")
            return sort_outlets_by_distance(user_latitude, user_longitude, outlets)
        
        user_country = user_location_info.get("country")
        user_city = user_location_info.get("city")
        
        print(f"[INFO] User location: {user_city}, {user_country}")
        
        # Filter outlets by location
        filtered_outlets = []
        same_city_outlets = []
        same_country_outlets = []
        other_outlets = []
        
        for outlet in outlets:
            outlet_lat = outlet.get("latitude")
            outlet_lon = outlet.get("longitude")
            
            if not outlet_lat or not outlet_lon:
                # Outlets without coordinates go to end
                other_outlets.append(outlet)
                continue
            
            # Get outlet location info
            outlet_location = get_location_info(outlet_lat, outlet_lon)
            
            if not outlet_location:
                other_outlets.append(outlet)
                continue
                
            outlet_country = outlet_location.get("country")
            outlet_city = outlet_location.get("city")
            
            # Add location info to outlet
            outlet_copy = dict(outlet)
            outlet_copy["location_info"] = outlet_location
            
            # Prioritize by location match
            if user_city and outlet_city and user_city.lower() == outlet_city.lower():
                same_city_outlets.append(outlet_copy)
            elif user_country and outlet_country and user_country.lower() == outlet_country.lower():
                same_country_outlets.append(outlet_copy)
            else:
                other_outlets.append(outlet_copy)
        
        # Sort each group by distance
        same_city_sorted = sort_outlets_by_distance(user_latitude, user_longitude, same_city_outlets)
        same_country_sorted = sort_outlets_by_distance(user_latitude, user_longitude, same_country_outlets) 
        other_sorted = sort_outlets_by_distance(user_latitude, user_longitude, other_outlets)
        
        # Combine: same city first, then same country, then others
        final_outlets = same_city_sorted + same_country_sorted + other_sorted
        
        print(f"[INFO] Filtered outlets: {len(same_city_sorted)} same city, {len(same_country_sorted)} same country, {len(other_sorted)} other")
        
        return final_outlets
        
    except Exception as e:
        print(f"[ERROR] Error filtering outlets by location: {str(e)}")
        # Fallback to simple distance sorting
        return sort_outlets_by_distance(user_latitude, user_longitude, outlets)
