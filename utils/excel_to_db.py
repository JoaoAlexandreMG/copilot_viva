import openpyxl
import csv
import os
from pathlib import Path
from models.models import User, Outlet, Asset, SmartDevice, Movement, HealthEvent, DoorEvent, Alert, Client, SubClient, AlertsDefinition
from datetime import datetime
from pytz import timezone
import pandas as pd
import numpy as np
import time

# Função helper para converter NaT para None
def convert_nat_to_none(df):
    """Converte pd.NaT para None em todas as colunas do DataFrame"""
    df = df.copy()
    # Usar replace do pandas para substituir NaT/NaN por None
    df = df.replace({pd.NaT: None, np.nan: None})
    return df

# Mapeamento entre os cabeçalhos do Excel e os atributos do modelo User
USER_COLUMN_MAPPING = {
    "First Name": "first_name",
    "Last Name": "last_name",
    "User Name": "user_name",
    "UPN": "upn",
    "Email": "email",
    "Phone": "phone",
    "Role": "role",
    "Reporting Manager": "reporting_manager",
    "Preferred Notification Type": "preferred_notification_type",
    "Country": "country",
    "Responsible Country": "responsible_country",
    "Is Active?": "is_active",
    "Sales Organization": "sales_organization",
    "Sales Office": "sales_office",
    "Sales Group": "sales_group",
    "Sales Territory": "sales_territory",
    "Teleselling Territory": "teleselling_territory",
    "BD Territory Name": "bd_territory_name",
    "CA Territory Name": "ca_territory_name",
    "MC Territory Name": "mc_territory_name",
    "P1 Territory Name": "p1_territory_name",
    "P2 Territory Name": "p2_territory_name",
    "P3 Territory Name": "p3_territory_name",
    "P4 Territory Name": "p4_territory_name",
    "P5 Territory Name": "p5_territory_name",
    "NCB Territory Name": "ncb_territory_name",
    "Last Login On": "last_login_on",
    "Client": "client",
    "Created On": "created_on",
    "Created By": "created_by",
    "Modified On": "modified_on",
    "Modified By": "modified_by",
    "Reward Point": "reward_point"
}

OUTLET_COLUMN_MAPPING = {
    "Name": "name",
    "Code": "code",
    "Outlet Type": "outlet_type",
    "Is Key Outlet?": "is_key_outlet",
    "Is Smart?": "is_smart",
    "Country": "country",
    "State": "state",
    "City": "city",
    "Street": "street",
    "Address 2": "address_2",
    "Address 3": "address_3",
    "Address 4": "address_4",
    "Postal Code": "postal_code",
    "Retailer": "retailer",
    "Primary Phone": "primary_phone",
    "Primary Sales Rep": "primary_sales_rep",
    "Sales Rep Name": "sales_rep_name",
    "Technician": "technician",
    "Market": "market",
    "Sales Target": "sales_target",
    "Client": "client",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Trade Channel": "trade_channel",
    "Trade Group": "trade_group",
    "Trade Group Code": "trade_group_code",
    "Is Active?": "is_active",
    "Customer Tier": "customer_tier",
    "Sub Trade Channel": "sub_trade_channel",
    "Sales Organization": "sales_organization",
    "Sales Office": "sales_office",
    "Sales Group": "sales_group",
    "Sales Territory": "sales_territory",
    "TeleSelling Territory Name": "teleselling_territory_name",
    "Business Developer Territory Name": "business_developer_territory_name",
    "Credit Approver Territory Name": "credit_approver_territory_name",
    "Merchandizer Territory Name": "merchandizer_territory_name",
    "P1_Territory Name": "p1_territory_name",
    "P2_Territory Name": "p2_territory_name",
    "P3_Territory Name": "p3_territory_name",
    "P4_Territory Name": "p4_territory_name",
    "P5_Territory Name": "p5_territory_name",
    "Reserve Route Name": "reserve_route_name",
    "RDCustomer Name": "rd_customer_name",
    "TimeZone": "time_zone",
    "Sub Client": "sub_client",
    "Cluster": "cluster",
    "Market Segment": "market_segment",
    "Segment": "segment",
    "Environment": "environment",
    "Assortment 1": "assortment_1",
    "Assortment 2": "assortment_2",
    "Assortment 3": "assortment_3",
    "Assortment 4": "assortment_4",
    "Assortment 5": "assortment_5",
    "BarCode": "barcode",
    "Local Cluster": "local_cluster",
    "Local TradeChannel": "local_trade_channel",
    "Chain": "chain",
    "Region Name": "region_name",
    "Mobile Phone": "mobile_phone",
    "Email": "email",
    "CPL Name": "cpl_name",
    "Extra Field": "extra_field",
    "Created On": "created_on",
    "Created By": "created_by",
    "Modified On": "modified_on",
    "Modified By": "modified_by",
    "BDAA": "bdaa",
    "CMMIND": "cmmind",
    "Combined Asset Capacity": "combined_asset_capacity",
    "ASM Name": "asm_name",
    "ASM Email": "asm_email",
    "TSM Name": "tsm_name",
    "TSM Email": "tsm_email"
}

ASSET_COLUMN_MAPPING = {
    "Asset Type": "asset_type",
    "Bottler Equipment Number": "bottler_equipment_number",
    "Technical Id": "technical_id",
    "OEM Serial Number": "oem_serial_number",
    "Asset Ping": "asset_ping",
    "Category": "category",
    "Is Competition?": "is_competition",
    "Is Factory Asset?": "is_factory_asset",
    "Associated in Factory": "associated_in_factory",
    "Outlet": "outlet",
    "Outlet Code": "outlet_code",
    "Outlet Type": "outlet_type",
    "Store Location": "store_location",
    "Trade Channel": "trade_channel",
    "Customer Tier": "customer_tier",
    "Sub Trade Channel": "sub_trade_channel",
    "Sales Organization": "sales_organization",
    "Sales Office": "sales_office",
    "Sales Group": "sales_group",
    "Sales Territory": "sales_territory",
    "Issue": "issue",
    "Smart Device": "smart_device",
    "Smart Device Type": "smart_device_type",
    "Smart Device Ping": "smart_device_ping",
    "Gateway": "gateway",
    "Gateway Type": "gateway_type",
    "Gateway Ping": "gateway_ping",
    "Last Scan": "last_scan",
    "Visit (Scan) Status": "visit_scan_status",
    "Client": "client",
    "City": "city",
    "Street": "street",
    "Street 2": "street_2",
    "Street 3": "street_3",
    "State": "state",
    "Country": "country",
    "Prime position?": "prime_position",
    "Is Missing?": "is_missing",
    "Is Vision?": "is_vision",
    "Is Smart?": "is_smart",
    "Is Authorized Movement ?": "is_authorized_movement",
    "Is Unhealthy?": "is_unhealthy",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Last Known Latitude": "last_known_latitude",
    "Last Known Longitude": "last_known_longitude",
    "Geolocation Source": "geolocation_source",
    "Location Accuracy": "location_accuracy",
    "Displacement(Meter)": "displacement_meter",
    "Is Power On?": "is_power_on",
    "Latest Health Record Event Time": "latest_health_record_event_time",
    "Battery Level": "battery_level",
    "Battery Status": "battery_status",
    "Planogram": "planogram",
    "Responsible BD Username": "responsible_bd_username",
    "Responsible BD First Name": "responsible_bd_first_name",
    "Responsible BD Phone number": "responsible_bd_phone_number",
    "IOT Solution": "iot_solution",
    "Has Sim": "has_sim",
    "Asset Associated On": "asset_associated_on",
    "Gateway Associated On": "gateway_associated_on",
    "Acquisition Date": "acquisition_date",
    "Associated By BD User Name": "associated_by_bd_user_name",
    "Associated By BD Name": "associated_by_bd_name",
    "Gateway Associated By BD User Name": "gateway_associated_by_bd_user_name",
    "Gateway Associated By BD Name": "gateway_associated_by_bd_name",
    "Time Zone": "time_zone",
    "Created On": "created_on",
    "Created By": "created_by",
    "Modified On": "modified_on",
    "Modified By": "modified_by",
    "Capacity Type": "capacity_type",
    "Sub Client": "sub_client",
    "Latest Movement Record Event Time": "latest_movement_record_event_time",
    "Latest Power Record Event Time": "latest_power_record_event_time",
    "Location status": "location_status",
    "Last Location Status On": "last_location_status_on",
    "Latest Location Status On": "latest_location_status_on",
    "Static Latitude": "static_latitude",
    "Static Longitude": "static_longitude",
    "Static Movement Status": "static_movement_status"
}

SMARTDEVICE_COLUMN_MAPPING = {
    "Device Type": "device_type",
    "Manufacturer": "manufacturer",
    "Mac Address": "mac_address",
    "Serial Number": "serial_number",
    "Order Serial Number": "order_serial_number",
    "Shipped Country": "shipped_country",
    "Door No": "door_no",
    "Bottler Equipment Number": "bottler_equipment_number",
    "Technical Identification Number": "technical_identification_number",
    "Gateway": "gateway",
    "Manufacturer Serial Number": "manufacturer_serial_number",
    "IMEI": "imei",
    "Sim#": "sim_number",
    "SIM Provider": "sim_provider",
    "Plugin Connected_FFXy": "plugin_connected_ffxy",
    "Last Ping": "last_ping",
    "Firmware Version": "firmware_version",
    "IBeacon UUID": "ibeacon_uuid",
    "IBeacon Major": "ibeacon_major",
    "IBeacon Minor": "ibeacon_minor",
    "Eddystone UID Namespace": "eddystone_uid_namespace",
    "Eddystone UID Instance": "eddystone_uid_instance",
    "Inventory Location": "inventory_location",
    "Tracking#": "tracking_number",
    "Client": "client",
    "Asset Type": "asset_type",
    "Linked with Asset": "linked_with_asset",
    "Is Factory Asset?": "is_factory_asset",
    "Associated in Factory": "associated_in_factory",
    "Acquisition Date": "acquisition_date",
    "Asset Associated On": "asset_associated_on",
    "Association": "association",
    "Associated By BD User Name": "associated_by_bd_user_name",
    "Associated By BD Name": "associated_by_bd_name",
    "Associated By App Version": "associated_by_app_version",
    "Associated By App Name": "associated_by_app_name",
    "Is Missing?": "is_missing",
    "Outlet": "outlet",
    "Outlet Code": "outlet_code",
    "Outlet Type": "outlet_type",
    "Trade Channel": "trade_channel",
    "Customer Tier": "customer_tier",
    "Sub Trade Channel": "sub_trade_channel",
    "Sales Organization": "sales_organization",
    "Sales Office": "sales_office",
    "Sales Group": "sales_group",
    "Sales Territory": "sales_territory",
    "Street": "street",
    "City": "city",
    "State": "state",
    "Country": "country",
    "Time Zone": "time_zone",
    "Latest Health Record Event Time": "latest_health_record_event_time",
    "Battery Level": "battery_level",
    "Created On": "created_on",
    "Created By": "created_by",
    "Modified On": "modified_on",
    "Modified By": "modified_by",
    "Advertisement URL": "advertisement_url",
    "Is Device Registered in IoT Hub?": "is_device_registered_in_iot_hub",
    "Is SD Gateway": "is_sd_gateway",
    "SubClient": "sub_client",
    "Device Model Number": "device_model_number",
    "Module Type": "module_type",
    "SIM Status": "sim_status",
    "Last Sim Status Updated On": "last_sim_status_updated_on"
}

MOVEMENT_COLUMN_MAPPING = {
    "Id": "id",
    "Movement Type": "movement_type",
    "Start Time": "start_time",
    "End Time": "end_time",
    "Duration": "duration",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Movement Count": "movement_count",
    "Door Open": "door_open",
    "Displacement(Meter)": "displacement_meter",
    "Accuracy(Meter)": "accuracy_meter",
    "Power Status": "power_status",
    "App Name": "app_name",
    "App Version": "app_version",
    "SDK Version": "sdk_version",
    "Data Uploaded By": "data_uploaded_by",
    "GPS Source": "gps_source",
    "Event Id": "event_id",
    "Created On": "created_on",
    "Gateway Mac": "gateway_mac",
    "Gateway#": "gateway_number",
    "Asset Type": "asset_type",
    "Month": "month",
    "Day ": "day",
    "Day of Week": "day_of_week",
    "Week of Year": "week_of_year",
    "Asset Serial #": "asset_serial_number",
    "Technical Id": "technical_id",
    "Equipment Number": "equipment_number",
    "Smart Device Mac": "smart_device_mac",
    "Smart Device#": "smart_device_number",
    "Is Smart?": "is_smart",
    "Smart Device Type": "smart_device_type",
    "Outlet": "outlet",
    "Outlet Code": "outlet_code",
    "Outlet Type": "outlet_type",
    "Time Zone": "time_zone",
    "Client": "client",
    "Sub Client": "sub_client"
}

HEALTH_EVENT_COLUMN_MAPPING = {
    "Id": "id",
    "Event Type": "event_type",
    "Light": "light",
    "Light Status": "light_status",
    "Temperature(°C)": "temperature_c",
    "Evaporator Temperature(°C)": "evaporator_temperature_c",
    "Condensor Temperature(°C)": "condensor_temperature_c",
    "Temperature(°F)": "temperature_f",
    "Evaporator Temperature(°F)": "evaporator_temperature_f",
    "Condensor Temperature(°F)": "condensor_temperature_f",
    "Battery": "battery",
    "Battery Status": "battery_status",
    "Interval(Min)": "interval_min",
    "Cooler Voltage(V)": "cooler_voltage_v",
    "Max Voltage(V)": "max_voltage_v",
    "Min Voltage(V)": "min_voltage_v",
    "Avg Power Consumption(Watt)": "avg_power_consumption_watt",
    "Total compressor ON Time(%)": "total_compressor_on_time_percent",
    "Max Cabinet Temperature(°C)": "max_cabinet_temperature_c",
    "Min Cabinet Temperature(°C)": "min_cabinet_temperature_c",
    "Ambient Temperature(°C)": "ambient_temperature_c",
    "Max Cabinet Temperature(°F)": "max_cabinet_temperature_f",
    "Min Cabinet Temperature(°F)": "min_cabinet_temperature_f",
    "Ambient Temperature(°F)": "ambient_temperature_f",
    "App Name": "app_name",
    "App Version": "app_version",
    "SDK Version": "sdk_version",
    "Data Uploaded By": "data_uploaded_by",
    "Asset Category": "asset_category",
    "Event Id": "event_id",
    "Created On": "created_on",
    "Event Time": "event_time",
    "Gateway Mac": "gateway_mac",
    "Gateway#": "gateway_number",
    "Asset Type": "asset_type",
    "Month": "month",
    "Day ": "day",
    "Day of Week": "day_of_week",
    "Week of Year": "week_of_year",
    "Asset Serial #": "asset_serial_number",
    "Technical Id": "technical_id",
    "Equipment Number": "equipment_number",
    "Smart Device Mac": "smart_device_mac",
    "Smart Device#": "smart_device_number",
    "Is Smart?": "is_smart",
    "Smart Device Type": "smart_device_type",
    "Outlet": "outlet",
    "Outlet Code": "outlet_code",
    "Outlet Type": "outlet_type",
    "Time Zone": "time_zone",
    "Client": "client",
    "Sub Client": "sub_client"
}

DOOR_EVENT_COLUMN_MAPPING = {
    "Id": "id",
    "Open Event Time": "open_event_time",
    "Close Event Time": "close_event_time",
    "Event Type": "event_type",
    "Door Open Duration(sec)": "door_open_duration_sec",
    "Time of Day": "time_of_day",
    "Weekday / Weekend": "weekday_weekend",
    "Hour in Day": "hour_in_day",
    "Door Count": "door_count",
    "Additional Info": "additional_info",
    "Outlet Territory": "outlet_territory",
    "Door": "door",
    "Capacity Type": "capacity_type",
    "Door Open Target": "door_open_target",
    "Door Open Temperature": "door_open_temperature",
    "Door Close Temperature": "door_close_temperature",
    "App Name": "app_name",
    "App Version": "app_version",
    "SDK Version": "sdk_version",
    "Data Uploaded By": "data_uploaded_by",
    "Asset Category": "asset_category",
    "Event Id": "event_id",
    "Created On": "created_on",
    "Gateway Mac": "gateway_mac",
    "Gateway#": "gateway_number",
    "Asset Type": "asset_type",
    "Month": "month",
    "Day ": "day",
    "Day of Week": "day_of_week",
    "Week of Year": "week_of_year",
    "Asset Serial #": "asset_serial_number",
    "Technical Id": "technical_id",
    "Equipment Number": "equipment_number",
    "Smart Device Mac": "smart_device_mac",
    "Smart Device#": "smart_device_number",
    "Is Smart?": "is_smart",
    "Smart Device Type": "smart_device_type",
    "Outlet": "outlet",
    "Outlet Code": "outlet_code",
    "Outlet Type": "outlet_type",
    "Time Zone": "time_zone",
    "Client": "client",
    "Sub Client": "sub_client"
}

ALERT_COLUMN_MAPPING = {
    "Id": "id",
    "Alert Type": "alert_type",
    "Alert Text": "alert_text",
    "Alert Definition": "alert_definition",
    "Status": "status",
    "Visit Check": "visit_check",
    "Asset Serial#": "asset_serial_number",
    "Smart Device Serial#": "smart_device_serial_number",
    "Asset Equipment Number": "asset_equipment_number",
    "Asset Technical Identification Number": "asset_technical_identification_number",
    "Asset Type": "asset_type",
    "Street": "street",
    "Street 2": "street_2",
    "Street 3": "street_3",
    "Is Smart?": "is_smart",
    "Alert At": "alert_at",
    "Status Changed On": "status_changed_on",
    "Priority": "priority",
    "Age": "age",
    "Alert Age(in minutes)": "alert_age_in_minutes",
    "Value": "value",
    "Last Update": "last_update",
    "Outlet": "outlet",
    "Outlet Code": "outlet_code",
    "Outlet Type": "outlet_type",
    "Outlet City": "outlet_city",
    "Client": "client",
    "Time Zone": "time_zone",
    "Month": "month",
    "Day ": "day",
    "Day of Week": "day_of_week",
    "Week of Year": "week_of_year",
    "Market": "market",
    "Trade Channel": "trade_channel",
    "Customer Tier": "customer_tier",
    "Sales Organization": "sales_organization",
    "Sales Office": "sales_office",
    "Sales Group": "sales_group",
    "Sales Territory": "sales_territory",
    "Sales Rep": "sales_rep",
    "Is System Alert?": "is_system_alert",
    "Acknowledge Comment": "acknowledge_comment",
    "Created On": "created_on"
}

CLIENT_COLUMN_MAPPING = {
    "Client Id": "id",
    "Client Code": "client_code",
    "Client Name": "client_name",
    "Relevant Business Stream": "relevant_business_stream",
    "Status": "status",
    "Contact": "contact",
    "Subdomain": "subdomain",
    "Is Feedback Enabled?": "is_feedback_enabled",
    "Time Zone": "time_zone",
    "Vision Image Interval (Hours)": "vision_image_interval_hours",
    "Vision Image Interval (Door Open)": "vision_image_interval_door_open",
    "Out Of Stock SKU": "out_of_stock_sku",
    "Power Off Duration": "power_off_duration",
    "Temperature Min": "temperature_min",
    "Temperature Max": "temperature_max",
    "Light Min": "light_min",
    "Light Max": "light_max",
    "Door Count": "door_count",
    "Health Intervals  (Hours)": "health_intervals_hours",
    "Cooler Tracking Threshold (Days)": "cooler_tracking_threshold_days",
    "Cooler Tracking Displacement Threshold (Mtr)": "cooler_tracking_displacement_threshold_mtr",
    "GeoLocation Api Key": "geolocation_api_key",
    "Created On": "created_on",
    "Created By": "created_by",
    "Modified On": "modified_on",
    "Modified By": "modified_by",
    "Fallen Magnet Threshold": "fallen_magnet_threshold",
    "VHenabled": "vh_enabled",
    "Country": "country",
    "Shipped Country": "shipped_country",
    "Manual Processing Mode": "manual_processing_mode",
    "Is Visit From Ping": "is_visit_from_ping",
    "Distance In Meter": "distance_in_meter",
    "Threshold In Minutes": "threshold_in_minutes",
    "Limit Location Distance": "limit_location_distance",
    "Survey Distance ": "survey_distance",
    "Scene Mode": "scene_mode",
    "Currency": "currency",
    "Enable PIC To POG": "enable_pic_to_pog",
    "Role": "role",
    "Default Recognition Mode": "default_recognition_mode",
    "Disable Geo Data Collection": "disable_geo_data_collection"
}

SUBCLIENT_COLUMN_MAPPING = {
    "SubClient Name": "subclient_name",
    "SubClient Code": "subclient_code",
    "Scene Mode": "scene_mode",
    "Client": "client"
}

ALERTS_DEFINITION_COLUMN_MAPPING = {
    "Name": "name",
    "Type": "type",
    "Asset Serial Number": "asset_serial_number",
    "Priority": "priority",
    "Is Active": "is_active",
    "Open Alert": "open_alert",
    "Updated Alert": "updated_alert",
    "Movement Detected": "movement_detected",
    "Power Off Duration": "power_off_duration",
    "Temperature Below": "temperature_below",
    "Temperature Above": "temperature_above",
    "Offline Alert Time": "offline_alert_time",
    "Online Alert Time": "online_alert_time",
    "Missing/Faulty time": "missing_faulty_time",
    "Cooler Disconnect Threshold": "cooler_disconnect_threshold",
    "Alert Age Threshold": "alert_age_threshold",
    "Prolonged Irregularity(Min)": "prolonged_irregularity_min",
    "No Data Threshold": "no_data_threshold",
    "Battery Open Threshold": "battery_open_threshold",
    "Battery Close Threshold": "battery_close_threshold",
    "Stock Threshold": "stock_threshold",
    "Purity Threshold": "purity_threshold",
    "Planogram Threshold": "planogram_threshold",
    "GPS Displacement Threshold": "gps_displacement_threshold",
    "Motion Available Time": "motion_available_time",
    "Par Displacement (Meter)": "par_displacement_meter",
    "Colas Threshold": "colas_threshold",
    "Flavours Threshold": "flavours_threshold",
    "Colas + Flavours": "colas_flavours",
    "Lane Threshold": "lane_threshold",
    "Min Stock": "min_stock",
    "Alert Text": "alert_text",
    "Daily Alert": "daily_alert",
    "Client": "client",
    "Outlet": "outlet",
    "Sales Organization": "sales_organization",
    "Trade Channel": "trade_channel",
    "City": "city",
    "State": "state",
    "SalesRep": "sales_rep",
    "Supervisor": "supervisor",
    "Solution Type": "solution_type",
    "Is System Alert?": "is_system_alert",
    "Created On": "created_on",
    "Created By": "created_by",
    "Modified On": "modified_on",
    "Modified By": "modified_by"
}

def parse_datetime(value, verbose=False):
    if isinstance(value, datetime):  # Verificar se já é um objeto datetime
        return value
    if value:
        try:
            dt_str = str(value).strip()  # Remove espaços no início e final
            
            # Se contém fuso horário no final, remover
            # Padrões: "BRST", "ESAST", "EDT", "EST", etc. (3-5 letras maiúsculas)
            parts = dt_str.split()
            detected_tz = None
            if len(parts) > 1 and parts[-1].isupper() and len(parts[-1]) in [3, 4, 5]:
                # Capturar o fuso horário antes de remover
                tz_part = parts[-1]
                # Remover o fuso horário (última parte se for tudo maiúsculo)
                dt_str = " ".join(parts[:-1]).strip()
                
                # Mapear fusos horários conhecidos
                tz_mapping = {
                    "BRST": "America/Sao_Paulo",      # Brasília Summer Time
                    "BRT": "America/Sao_Paulo",       # Brasília Time  
                    "ESAST": "Africa/Johannesburg",   # East South Africa Standard Time
                    "SAST": "Africa/Johannesburg",    # South Africa Standard Time
                    "EST": "America/New_York",        # Eastern Standard Time
                    "EDT": "America/New_York",        # Eastern Daylight Time
                    "PST": "America/Los_Angeles",     # Pacific Standard Time
                    "PDT": "America/Los_Angeles",     # Pacific Daylight Time
                    "UTC": "UTC",                     # Universal Time Coordinated
                    "GMT": "UTC"                      # Greenwich Mean Time
                }
                detected_tz = tz_mapping.get(tz_part, "America/Sao_Paulo")
            
            dt = None
            
            # Tentar múltiplos formatos de data
            formats = [
                "%d/%m/%Y %H:%M:%S.%f",  # DD/MM/YYYY HH:MM:SS.sss
                "%d/%m/%Y %H:%M:%S",  # DD/MM/YYYY HH:MM:SS (padrão brasileiro)
                "%d/%m/%Y %H:%M:%S%z",
                "%m/%d/%Y %H:%M:%S.%f",  # MM/DD/YYYY HH:MM:SS.sss (padrão americano)
                "%m/%d/%Y %H:%M:%S",  # MM/DD/YYYY HH:MM:SS (padrão americano)
                "%m/%d/%Y %H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S.%f",  # YYYY-MM-DD HH:MM:SS.sss (ISO)
                "%Y-%m-%d %H:%M:%S",  # YYYY-MM-DD HH:MM:SS (ISO)
                "%Y-%m-%d %H:%M:%S%z",
                "%d/%m/%Y",           # DD/MM/YYYY (apenas data)
                "%m/%d/%Y",           # MM/DD/YYYY (apenas data)
                "%Y-%m-%d",           # YYYY-MM-DD (apenas data)
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(dt_str, fmt)
                    if verbose:
                        print(f"✅ Data convertida: '{value}' -> {dt} usando formato {fmt}")
                    break
                except ValueError as format_error:
                    continue
            
            if dt is None:
                if verbose:
                    print(f"⚠️  Não foi possível converter a data: {value} | String processada: '{dt_str}'")
                return None
            
            # Adicionar o fuso horário apropriado
            # Se o dt já possui tzinfo, retorná-lo diretamente
            if dt.tzinfo is not None:
                return dt

            tz_name = detected_tz if detected_tz else "America/Sao_Paulo"
            try:
                tz = timezone(tz_name)
                localized_dt = tz.localize(dt)
                if verbose and detected_tz:
                    print(f"✅ Fuso horário aplicado: {tz_name}")
                return localized_dt
            except Exception as tz_error:
                if verbose:
                    print(f"⚠️  Erro no fuso horário {tz_name}: {tz_error}. Usando América/São_Paulo")
                tz = timezone("America/Sao_Paulo")
                return tz.localize(dt)
                
        except Exception as e:
            if verbose:
                print(f"❌ Erro ao converter data '{value}': {e}")
            return None
    return None

# Função para inserir ou atualizar usuários a partir do Excel - OTIMIZADO
def insert_or_update_users_from_excel(session, excel_file_path):
    """VERSÃO OTIMIZADA COM PANDAS - SÓ INSERT, SEM UPDATE LENTO"""
    start_time = time.time()
    print(f"🚀 PANDAS ULTRA (Users): {excel_file_path}")
    
    # 1. Ler Excel - PRESERVAR NULL
    df = pd.read_excel(excel_file_path, engine='openpyxl', keep_default_na=True, na_values=['', 'nan', 'NaN', 'N/A'])
    print(f"📁 Lido: {len(df)} registros em {time.time() - start_time:.2f}s")
    
    # 2. Renomear colunas conforme mapeamento
    df_columns = {col: USER_COLUMN_MAPPING[col] for col in df.columns if col in USER_COLUMN_MAPPING}
    df = df.rename(columns=df_columns)
    
    # 3. Garantir coluna upn
    if 'upn' not in df.columns:
        print("❌ Coluna 'upn' não encontrada")
        return
    
    df = df.dropna(subset=['upn'])
    df['upn'] = df['upn'].astype(str).str.strip()
    
    # 4. Processar datas
    for col in ['last_login_on', 'created_on', 'modified_on']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: parse_datetime(str(x).strip()) if pd.notna(x) else None)
    
    # 5. Processar booleano
    if 'is_active' in df.columns:
        df['is_active'] = df['is_active'].apply(lambda x: True if str(x).strip().lower() == 'yes' else False if pd.notna(x) else None)
    
    # 6. PRESERVAR NULL - Não usar df.where() que pode converter NULL para 0 em colunas numéricas
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or (isinstance(x, str) and x.strip() == '') else x)
    
    print(f"🧹 Processado: {len(df)} registros")
    
    # 6. SÓ NOVOS (insert rápido)
    existing_upns = {row.upn for row in session.query(User.upn).all()}
    df_insert = df[~df['upn'].isin(existing_upns)]
    
    if len(df_insert) > 0:
        df_insert = convert_nat_to_none(df_insert)
        session.bulk_insert_mappings(User, df_insert.to_dict('records'))
        session.commit()
        print(f"📥 Inseridos: {len(df_insert)} registros")
    else:
        print("📥 Nenhum registro novo")
    
    total_time = time.time() - start_time
    speed = len(df) / total_time if total_time > 0 else 0
    print(f"🎉 {total_time:.2f}s | ⚡ {speed:.0f} reg/s")

# Função para inserir ou atualizar outlets a partir do Excel - OTIMIZADO
def insert_or_update_outlets_from_excel(session, excel_file_path):
    """VERSÃO OTIMIZADA COM PANDAS - SÓ INSERT, SEM UPDATE LENTO"""
    start_time = time.time()
    print(f"🚀 PANDAS ULTRA (Outlets): {excel_file_path}")
    
    # 1. Ler Excel - PRESERVAR NULL
    df = pd.read_excel(excel_file_path, engine='openpyxl', keep_default_na=True, na_values=['', 'nan', 'NaN', 'N/A'])
    print(f"📁 Lido: {len(df)} registros em {time.time() - start_time:.2f}s")
    
    # 2. Renomear colunas conforme mapeamento
    df_columns = {col: OUTLET_COLUMN_MAPPING[col] for col in df.columns if col in OUTLET_COLUMN_MAPPING}
    df = df.rename(columns=df_columns)
    
    # 3. Garantir coluna code
    if 'code' not in df.columns:
        print("❌ Coluna 'code' não encontrada")
        return
    
    df = df.dropna(subset=['code'])
    df['code'] = df['code'].astype(str).str.strip()
    
    # 4. Processar datas
    for col in ['created_on', 'modified_on']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: parse_datetime(str(x).strip()) if pd.notna(x) else None)
    
    # 5. Processar booleanos
    for col in ['is_key_outlet', 'is_smart', 'is_active']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: True if str(x).strip().lower() == 'yes' else False if pd.notna(x) else None)
    
    # 6. PRESERVAR NULL - Não usar df.where() que pode converter NULL para 0 em colunas numéricas
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or (isinstance(x, str) and x.strip() == '') else x)
    
    print(f"🧹 Processado: {len(df)} registros")
    
    # 6. SÓ NOVOS (insert rápido)
    existing_codes = {row.code for row in session.query(Outlet.code).all()}
    df_insert = df[~df['code'].isin(existing_codes)]
    
    if len(df_insert) > 0:
        df_insert = convert_nat_to_none(df_insert)
        session.bulk_insert_mappings(Outlet, df_insert.to_dict('records'))
        session.commit()
        print(f"📥 Inseridos: {len(df_insert)} registros")
    else:
        print("📥 Nenhum registro novo")
    
    total_time = time.time() - start_time
    speed = len(df) / total_time if total_time > 0 else 0
    print(f"🎉 {total_time:.2f}s | ⚡ {speed:.0f} reg/s")

# Função para inserir ou atualizar assets a partir do Excel - OTIMIZADO
def insert_or_update_assets_from_excel(session, excel_file_path):
    """VERSÃO OTIMIZADA COM PANDAS - SÓ INSERT, SEM UPDATE LENTO"""
    start_time = time.time()
    print(f"🚀 PANDAS ULTRA (Assets): {excel_file_path}")
    
    # 1. Ler Excel - PRESERVAR NULL
    df = pd.read_excel(excel_file_path, engine='openpyxl', keep_default_na=True, na_values=['', 'nan', 'NaN', 'N/A'])
    print(f"📁 Lido: {len(df)} registros em {time.time() - start_time:.2f}s")
    
    # 2. Renomear colunas conforme mapeamento
    df_columns = {col: ASSET_COLUMN_MAPPING[col] for col in df.columns if col in ASSET_COLUMN_MAPPING}
    df = df.rename(columns=df_columns)
    
    # 3. Garantir coluna oem_serial_number
    if 'oem_serial_number' not in df.columns:
        print("❌ Coluna 'oem_serial_number' não encontrada")
        return
    
    df = df.dropna(subset=['oem_serial_number'])
    df['oem_serial_number'] = df['oem_serial_number'].astype(str).str.strip()
    
    # 4. Processar datas
    date_cols = ['last_scan', 'asset_associated_on', 'gateway_associated_on', 'acquisition_date', 
                 'created_on', 'modified_on', 'latest_health_record_event_time', 
                 'latest_movement_record_event_time', 'latest_power_record_event_time', 
                 'last_location_status_on', 'latest_location_status_on']
    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: parse_datetime(str(x).strip()) if pd.notna(x) else None)
    
    # 5. Processar booleanos
    bool_cols = ['is_competition', 'is_factory_asset', 'associated_in_factory', 'prime_position',
                 'is_missing', 'is_vision', 'is_smart', 'is_authorized_movement',
                 'is_unhealthy', 'is_power_on', 'has_sim']
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: True if str(x).strip().lower() == 'yes' else False if pd.notna(x) else None)
    
    # 6. PRESERVAR NULL - Não usar df.where() que pode converter NULL para 0 em colunas numéricas
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or (isinstance(x, str) and x.strip() == '') else x)
    
    print(f"🧹 Processado: {len(df)} registros")
    
    # 6. SÓ NOVOS (insert rápido)
    existing_serials = {row.oem_serial_number for row in session.query(Asset.oem_serial_number).all()}
    df_insert = df[~df['oem_serial_number'].isin(existing_serials)]
    
    if len(df_insert) > 0:
        df_insert = convert_nat_to_none(df_insert)
        session.bulk_insert_mappings(Asset, df_insert.to_dict('records'))
        session.commit()
        print(f"📥 Inseridos: {len(df_insert)} registros")
    else:
        print("📥 Nenhum registro novo")
    
    total_time = time.time() - start_time
    speed = len(df) / total_time if total_time > 0 else 0
    print(f"🎉 {total_time:.2f}s | ⚡ {speed:.0f} reg/s")

# Função para inserir ou atualizar smart devices a partir do Excel - OTIMIZADO
def insert_or_update_smartdevices_from_excel(session, excel_file_path):
    """VERSÃO OTIMIZADA COM PANDAS - SÓ INSERT, SEM UPDATE LENTO"""
    start_time = time.time()
    print(f"🚀 PANDAS ULTRA (SmartDevices): {excel_file_path}")
    
    # 1. Ler Excel - PRESERVAR NULL
    df = pd.read_excel(excel_file_path, engine='openpyxl', keep_default_na=True, na_values=['', 'nan', 'NaN', 'N/A'])
    print(f"📁 Lido: {len(df)} registros em {time.time() - start_time:.2f}s")
    
    # 2. Renomear colunas conforme mapeamento
    df_columns = {col: SMARTDEVICE_COLUMN_MAPPING[col] for col in df.columns if col in SMARTDEVICE_COLUMN_MAPPING}
    df = df.rename(columns=df_columns)
    
    # 3. Garantir coluna mac_address
    if 'mac_address' not in df.columns:
        print("❌ Coluna 'mac_address' não encontrada")
        return
    
    df = df.dropna(subset=['mac_address'])
    df['mac_address'] = df['mac_address'].astype(str).str.strip()
    
    # 4. Processar datas
    date_cols = ['last_ping', 'acquisition_date', 'asset_associated_on', 
                 'latest_health_record_event_time', 'created_on', 'modified_on', 
                 'last_sim_status_updated_on']
    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: parse_datetime(str(x).strip()) if pd.notna(x) else None)
    
    # 5. Processar booleanos
    bool_cols = ['plugin_connected_ffxy', 'is_factory_asset', 'associated_in_factory',
                 'is_missing', 'is_device_registered_in_iot_hub', 'is_sd_gateway']
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: True if str(x).strip().lower() == 'yes' else False if pd.notna(x) else None)
    
    # 6. PRESERVAR NULL - Não usar df.where() que pode converter NULL para 0 em colunas numéricas
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or (isinstance(x, str) and x.strip() == '') else x)
    
    print(f"🧹 Processado: {len(df)} registros")
    
    # 6. SÓ NOVOS (insert rápido)
    existing_macs = {row.mac_address for row in session.query(SmartDevice.mac_address).all()}
    df_insert = df[~df['mac_address'].isin(existing_macs)]
    
    if len(df_insert) > 0:
        df_insert = convert_nat_to_none(df_insert)
        session.bulk_insert_mappings(SmartDevice, df_insert.to_dict('records'))
        session.commit()
        print(f"📥 Inseridos: {len(df_insert)} registros")
    else:
        print("📥 Nenhum registro novo")
    
    total_time = time.time() - start_time
    speed = len(df) / total_time if total_time > 0 else 0
    print(f"🎉 {total_time:.2f}s | ⚡ {speed:.0f} reg/s")

# Função para inserir ou atualizar movements a partir do Excel - OTIMIZADO
def insert_or_update_movements_from_excel(session, excel_file_path):
    """VERSÃO OTIMIZADA COM PANDAS - SÓ INSERT, SEM UPDATE LENTO"""
    start_time = time.time()
    print(f"🚀 PANDAS ULTRA (Movements): {excel_file_path}")
    
    # 1. Ler Excel - PRESERVAR NULL
    df = pd.read_excel(excel_file_path, engine='openpyxl', keep_default_na=True, na_values=['', 'nan', 'NaN', 'N/A'])
    print(f"📁 Lido: {len(df)} registros em {time.time() - start_time:.2f}s")
    
    # 2. Renomear colunas conforme mapeamento
    reverse_mapping = {v: k for k, v in MOVEMENT_COLUMN_MAPPING.items()}
    df_columns = {col: MOVEMENT_COLUMN_MAPPING[col] for col in df.columns if col in MOVEMENT_COLUMN_MAPPING}
    df = df.rename(columns=df_columns)
    
    # 3. Garantir coluna id
    if 'id' not in df.columns:
        print("❌ Coluna 'id' não encontrada")
        return
    
    df = df.dropna(subset=['id'])
    def normalize_id(x):
        if pd.isna(x):
            return None
        try:
            if isinstance(x, float) and x.is_integer():
                return str(int(x))
        except Exception:
            pass
        return str(x).strip()
    df['id'] = df['id'].apply(normalize_id)
    
    # 4. Processar datas
    for col in ['start_time', 'end_time', 'created_on']:
        if col in df.columns:
            print(f"🔄 Processando coluna '{col}'...")
            invalid_count = 0
            valid_count = 0
            
            def parse_and_count(x):
                nonlocal invalid_count, valid_count
                result = parse_datetime(str(x).strip(), verbose=False) if pd.notna(x) else None
                if result is None and pd.notna(x):
                    invalid_count += 1
                elif result is not None:
                    valid_count += 1
                return result
            
            df[col] = df[col].apply(parse_and_count)
            print(f"   ✅ Válidos: {valid_count} | ⚠️  Inválidos: {invalid_count}")
    
    # 5. Processar booleanos
    for col in ['door_open', 'is_smart']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: True if str(x).strip().lower() == 'yes' else False if pd.notna(x) else None)
    
    # 6. PRESERVAR NULL - Não usar df.where() que pode converter NULL para 0 em colunas numéricas
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or (isinstance(x, str) and x.strip() == '') else x)
    
    # 6. FILTRAR: Remover registros com start_time NULL (obrigatório!)
    null_start_time_before = df[df['start_time'].isna()].shape[0]
    df = df.dropna(subset=['start_time'])
    null_start_time_after = df[df['start_time'].isna()].shape[0]
    
    if null_start_time_before > 0:
        print(f"⚠️  REMOVIDOS: {null_start_time_before} registros com 'start_time' NULL")
    
    print(f"🧹 Processado: {len(df)} registros válidos")
    
    # 7. INSERT/UPDATE (upsert)
    existing_ids = {row.id for row in session.query(Movement.id).all()}
    df_insert = df[~df['id'].isin(existing_ids)]
    df_update = df[df['id'].isin(existing_ids)]

    inserted = 0
    updated = 0
    if len(df_insert) > 0:
        df_insert = convert_nat_to_none(df_insert)
        session.bulk_insert_mappings(Movement, df_insert.to_dict('records'))
        inserted = len(df_insert)

    if len(df_update) > 0:
        df_update = df_update[df_update['id'].notna()].copy()
        df_update = convert_nat_to_none(df_update)
        update_records = []
        for rec in df_update.to_dict('records'):
            record = {k: v for k, v in rec.items() if v is not None}
            record['id'] = rec.get('id')
            update_records.append(record)
        if update_records:
            session.bulk_update_mappings(Movement, update_records)
            updated = len(update_records)

    if inserted or updated:
        session.commit()
    print(f"📥 Inseridos: {inserted} | 🔁 Atualizados: {updated}")
    return {"inserted": inserted, "updated": updated, "read": len(df)}
    
    total_time = time.time() - start_time
    speed = len(df) / total_time if total_time > 0 else 0
    print(f"🎉 {total_time:.2f}s | ⚡ {speed:.0f} reg/s")

# Função para inserir ou atualizar health events a partir do Excel - OTIMIZADO
def insert_or_update_health_events_from_excel(session, excel_file_path):
    """VERSÃO OTIMIZADA COM PANDAS - SÓ INSERT, SEM UPDATE LENTO"""
    start_time = time.time()
    print(f"🚀 PANDAS ULTRA (HealthEvents): {excel_file_path}")
    
    # 1. Ler Excel - PRESERVAR NULL (não converter para 0)
    df = pd.read_excel(excel_file_path, engine='openpyxl', keep_default_na=True, na_values=['', 'nan', 'NaN', 'N/A'])
    print(f"📁 Lido: {len(df)} registros em {time.time() - start_time:.2f}s")
    
    # 2. Renomear colunas conforme mapeamento
    df_columns = {col: HEALTH_EVENT_COLUMN_MAPPING[col] for col in df.columns if col in HEALTH_EVENT_COLUMN_MAPPING}
    df = df.rename(columns=df_columns)
    
    # 3. Garantir coluna id
    if 'id' not in df.columns:
        print("❌ Coluna 'id' não encontrada")
        return
    
    df = df.dropna(subset=['id'])
    df['id'] = df['id'].astype(str).str.strip()
    
    # 4. Processar datas
    for col in ['created_on', 'event_time']:
        if col in df.columns:
            print(f"🔄 Processando coluna '{col}'...")
            invalid_count = 0
            valid_count = 0
            
            def parse_and_count(x):
                nonlocal invalid_count, valid_count
                result = parse_datetime(str(x).strip(), verbose=False) if pd.notna(x) else None
                if result is None and pd.notna(x):
                    invalid_count += 1
                elif result is not None:
                    valid_count += 1
                return result
            
            df[col] = df[col].apply(parse_and_count)
            print(f"   ✅ Válidos: {valid_count} | ⚠️  Inválidos: {invalid_count}")
    
    # 5. Processar booleanos
    if 'is_smart' in df.columns:
        df['is_smart'] = df['is_smart'].apply(lambda x: True if str(x).strip().lower() == 'yes' else False if pd.notna(x) else None)
    
    # 6. PRESERVAR NULL - Não usar df.where() que pode converter NULL para 0 em colunas numéricas
    # Converter apenas campos de texto vazios para None, mantendo NULL em colunas numéricas
    for col in df.columns:
        if df[col].dtype == 'object':  # Apenas colunas de texto
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or (isinstance(x, str) and x.strip() == '') else x)
    
    print(f"🧹 Processado: {len(df)} registros")
    
    # 7. INSERT/UPDATE (upsert)
    existing_ids = {row.id for row in session.query(HealthEvent.id).all()}
    df_insert = df[~df['id'].isin(existing_ids)]
    df_update = df[df['id'].isin(existing_ids)]
    inserted = 0
    updated = 0
    if len(df_insert) > 0:
        # Verificar registros com event_time NULL
        null_event_time = df_insert[df_insert['event_time'].isna()].shape[0]
        if null_event_time > 0:
            print(f"⚠️  AVISO: {null_event_time} registros com 'event_time' NULL serão inseridos")
        
        # Verificar coluna battery para garantir NULL
        if 'battery' in df_insert.columns:
            null_battery = df_insert['battery'].isna().sum()
            print(f"📊 Coluna 'battery': {null_battery} NULL, {len(df_insert) - null_battery} com valor")
        
        df_insert = convert_nat_to_none(df_insert)
        session.bulk_insert_mappings(HealthEvent, df_insert.to_dict('records'))
        inserted = len(df_insert)

    if len(df_update) > 0:
        # ensure primary key present
        df_update = df_update[df_update['id'].notna()].copy()
        df_update = convert_nat_to_none(df_update)
        # Build update records including only non-null columns to avoid overwriting existing values with None
        update_records = []
        for rec in df_update.to_dict('records'):
            record = {k: v for k, v in rec.items() if v is not None}
            record['id'] = rec.get('id')
            update_records.append(record)
        if update_records:
            session.bulk_update_mappings(HealthEvent, update_records)
            updated = len(update_records)

    if inserted or updated:
        session.commit()
    print(f"📥 Inseridos: {inserted} | 🔁 Atualizados: {updated}")
    return {"inserted": inserted, "updated": updated, "read": len(df)}
    
    total_time = time.time() - start_time
    speed = len(df) / total_time if total_time > 0 else 0
    print(f"🎉 {total_time:.2f}s | ⚡ {speed:.0f} reg/s")

# VERSÃO OTIMIZADA PARA PROCESSAR TODAS AS COLUNAS COM TRATAMENTO DE EMPTY STRINGS
def insert_or_update_door_events_from_csv(session, csv_file_path):
    """VERSÃO OTIMIZADA COM PANDAS - SÓ INSERT, SEM UPDATE LENTO"""
    start_time = time.time()
    print(f"🚀 PANDAS ULTRA (DoorEvents CSV): {csv_file_path}")
    
    # 1. Ler CSV com quotechar e keep_default_na=False para não converter "" em NaN
    # Usar na_values vazio para evitar conversão automática
    df = pd.read_csv(csv_file_path, encoding='utf-16', skiprows=1, low_memory=False,
                     quotechar='"', keep_default_na=False, na_values=[''])
    print(f"📁 Lido: {len(df)} registros em {time.time() - start_time:.2f}s")
    
    # 2. Renomear colunas conforme mapeamento
    df_columns = {col: DOOR_EVENT_COLUMN_MAPPING[col] for col in df.columns if col in DOOR_EVENT_COLUMN_MAPPING}
    df = df.rename(columns=df_columns)
    
    # 3. Garantir coluna id
    if 'id' not in df.columns:
        print("❌ Coluna 'id' não encontrada")
        return
    
    df = df.dropna(subset=['id'])
    df['id'] = df['id'].astype(str).str.strip()
    
    # 4. Processar datas
    for col in ['open_event_time', 'close_event_time', 'created_on']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: parse_datetime(str(x).strip()) if pd.notna(x) and str(x).strip() else None)
    
    # 5. Processar booleanos
    if 'is_smart' in df.columns:
        df['is_smart'] = df['is_smart'].apply(lambda x: True if str(x).strip().lower() == 'yes' else False if pd.notna(x) and str(x).strip() else None)
    
    # 6. Limpar valores vazios para None
    df = df.where(pd.notnull(df) & (df != ''), None)
    print(f"🧹 Processado: {len(df)} registros")
    
    # 7. Mostrar estatísticas de NULL
    print(f"\n📈 Estatísticas de NULL por coluna:")
    null_stats = []
    for col in df.columns:
        null_count = df[col].isna().sum()
        null_pct = (null_count / len(df)) * 100
        if null_count > 0:
            null_stats.append((col, null_count, null_pct))
    
    # Mostrar colunas com NULL
    null_stats.sort(key=lambda x: x[1], reverse=True)
    for col, null_count, null_pct in null_stats[:10]:  # Top 10
        print(f"   {col}: {null_count} NULL ({null_pct:.1f}%)")
    
    if not null_stats:
        print("   ✅ Nenhuma coluna com NULL!")
    
    # 8. SÓ NOVOS (insert rápido)
    existing_ids = {row.id for row in session.query(DoorEvent.id).all()}
    df_insert = df[~df['id'].isin(existing_ids)]
    
    if len(df_insert) > 0:
        df_insert = convert_nat_to_none(df_insert)
        session.bulk_insert_mappings(DoorEvent, df_insert.to_dict('records'))
        session.commit()
        print(f"\n📥 Inseridos: {len(df_insert)} registros")
    else:
        print("\n📥 Nenhum registro novo")
    
    total_time = time.time() - start_time
    speed = len(df) / total_time if total_time > 0 else 0
    print(f"🎉 {total_time:.2f}s | ⚡ {speed:.0f} reg/s")

# Função para inserir ou atualizar health events a partir do CSV (UTF-16) - OTIMIZADO
def insert_or_update_health_events_from_csv(session, csv_file_path):
    """VERSÃO OTIMIZADA COM PANDAS - SÓ INSERT, SEM UPDATE LENTO"""
    start_time = time.time()
    print(f"🚀 PANDAS ULTRA (HealthEvents CSV): {csv_file_path}")
    
    # 1. Ler CSV - PRESERVAR NULL
    df = pd.read_csv(csv_file_path, encoding='utf-16', skiprows=1, low_memory=False, keep_default_na=True, na_values=['', 'nan', 'NaN', 'N/A'])
    print(f"📁 Lido: {len(df)} registros em {time.time() - start_time:.2f}s")
    
    # 2. Renomear colunas conforme mapeamento
    df_columns = {col: HEALTH_EVENT_COLUMN_MAPPING[col] for col in df.columns if col in HEALTH_EVENT_COLUMN_MAPPING}
    df = df.rename(columns=df_columns)
    
    # 3. Garantir coluna id
    if 'id' not in df.columns:
        print("❌ Coluna 'id' não encontrada")
        return
    
    df = df.dropna(subset=['id'])
    df['id'] = df['id'].astype(str).str.strip()
    
    # 4. Processar datas
    for col in ['created_on', 'event_time']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: parse_datetime(str(x).strip()) if pd.notna(x) else None)
    
    # 5. Processar booleanos
    if 'is_smart' in df.columns:
        df['is_smart'] = df['is_smart'].apply(lambda x: True if str(x).strip().lower() == 'yes' else False if pd.notna(x) else None)
    
    # 6. PRESERVAR NULL - Não usar df.where() que pode converter NULL para 0 em colunas numéricas
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or (isinstance(x, str) and x.strip() == '') else x)
    
    print(f"🧹 Processado: {len(df)} registros")
    
    # 7. INSERT/UPDATE (upsert)
    existing_ids = {row.id for row in session.query(HealthEvent.id).all()}
    df_insert = df[~df['id'].isin(existing_ids)]
    df_update = df[df['id'].isin(existing_ids)]
    
    inserted = 0
    updated = 0
    if len(df_insert) > 0:
        df_insert = convert_nat_to_none(df_insert)
        session.bulk_insert_mappings(HealthEvent, df_insert.to_dict('records'))
        inserted = len(df_insert)

    if len(df_update) > 0:
        # prepare updates - ensure primary key 'id' is present for bulk_update_mappings
        df_update = df_update[df_update['id'].notna()].copy()
        df_update = convert_nat_to_none(df_update)
        update_records = []
        for rec in df_update.to_dict('records'):
            record = {k: v for k, v in rec.items() if v is not None}
            record['id'] = rec.get('id')
            update_records.append(record)
        if update_records:
            session.bulk_update_mappings(HealthEvent, update_records)
            updated = len(update_records)

    if inserted or updated:
        session.commit()
    print(f"📥 Inseridos: {inserted} | 🔁 Atualizados: {updated}")
    return {"inserted": inserted, "updated": updated, "read": len(df)}
    
    total_time = time.time() - start_time
    speed = len(df) / total_time if total_time > 0 else 0
    print(f"🎉 {total_time:.2f}s | ⚡ {speed:.0f} reg/s")


def validate_health_events_import(session, csv_file_path, sample_limit=10):
    """Validate that health_events from CSV were imported correctly.
    For a sample of rows, assert that the 'id' matches and timestamps are identical (as datetime moments).
    Prints a short report for the first `sample_limit` rows.
    """
    import math
    print(f"🔍 Validando import de HealthEvents: {csv_file_path} (amostra: {sample_limit})")
    df = pd.read_csv(csv_file_path, encoding='utf-16', skiprows=1, low_memory=False, keep_default_na=True, na_values=['', 'nan', 'NaN', 'N/A'])
    df_columns = {col: HEALTH_EVENT_COLUMN_MAPPING[col] for col in df.columns if col in HEALTH_EVENT_COLUMN_MAPPING}
    df = df.rename(columns=df_columns)
    if 'id' not in df.columns:
        print("❌ Coluna 'id' não encontrada no CSV")
        return
    df['id'] = df['id'].astype(str).str.strip()
    # Prepare sample
    sample = df.head(sample_limit)
    failures = 0
    for i, row in sample.iterrows():
        csv_id = str(row['id']).strip()
        db_row = session.query(HealthEvent).filter(HealthEvent.id == csv_id).first()
        if not db_row:
            print(f"❌ ID {csv_id} não encontrado no DB")
            failures += 1
            continue
        # Compare event_time and created_on
        for col in ['event_time', 'created_on']:
            csv_val = row.get(col) if col in row.index else None
            parsed_csv_dt = parse_datetime(str(csv_val).strip(), verbose=False) if pd.notna(csv_val) else None
            db_dt = getattr(db_row, col)
            # Compare by normalized epoch seconds (UTC) when possible to avoid tz name differences
            if parsed_csv_dt is None and db_dt is None:
                continue
            if (parsed_csv_dt is None and db_dt is not None) or (parsed_csv_dt is not None and db_dt is None):
                print(f"⚠️  Mismatch for ID {csv_id} column {col}: CSV -> {parsed_csv_dt}, DB -> {db_dt}")
                failures += 1
                continue
            try:
                # convert both to UTC timestamps
                csv_ts = parsed_csv_dt.astimezone(timezone('UTC')).timestamp()
                db_ts = db_dt.astimezone(timezone('UTC')).timestamp() if db_dt.tzinfo else db_dt.replace(tzinfo=timezone('UTC')).timestamp()
                if math.isclose(csv_ts, db_ts, rel_tol=0, abs_tol=1e-3):
                    print(f"✅ ID {csv_id} {col} OK: {parsed_csv_dt.isoformat()} == {db_dt.isoformat()}")
                else:
                    print(f"❌ ID {csv_id} {col} DIFFER: CSV -> {parsed_csv_dt.isoformat()}, DB -> {db_dt.isoformat()}")
                    failures += 1
            except Exception as e:
                print(f"⚠️  Erro ao comparar datetime para ID {csv_id} coluna {col}: {e}")
                failures += 1
    print(f"🔎 Validação concluída: {len(sample)} amostras verificadas, {failures} falhas detectadas")


def validate_movements_import(session, excel_file_path, sample_limit=10):
    """Validate that movements from Excel were imported correctly.
    For a sample of rows, assert that the 'id' matches and timestamps (start_time, end_time, created_on) are identical.
    Prints a short report for the first `sample_limit` rows.
    """
    import math
    print(f"🔍 Validando import de Movements: {excel_file_path} (amostra: {sample_limit})")
    df = pd.read_excel(excel_file_path, engine='openpyxl', keep_default_na=True, na_values=['', 'nan', 'NaN', 'N/A'])
    df_columns = {col: MOVEMENT_COLUMN_MAPPING[col] for col in df.columns if col in MOVEMENT_COLUMN_MAPPING}
    df = df.rename(columns=df_columns)
    if 'id' not in df.columns:
        print("❌ Coluna 'id' não encontrada no Excel")
        return
    df['id'] = df['id'].astype(str).str.strip()
    sample = df.head(sample_limit)
    failures = 0
    for i, row in sample.iterrows():
        csv_id = str(row['id']).strip()
        db_row = session.query(Movement).filter(Movement.id == csv_id).first()
        if not db_row:
            print(f"❌ ID {csv_id} não encontrado no DB")
            failures += 1
            continue
        for col in ['start_time', 'end_time', 'created_on']:
            csv_val = row.get(col) if col in row.index else None
            parsed_csv_dt = parse_datetime(str(csv_val).strip(), verbose=False) if pd.notna(csv_val) else None
            db_dt = getattr(db_row, col)
            if parsed_csv_dt is None and db_dt is None:
                continue
            if (parsed_csv_dt is None and db_dt is not None) or (parsed_csv_dt is not None and db_dt is None):
                print(f"⚠️  Mismatch for ID {csv_id} column {col}: CSV -> {parsed_csv_dt}, DB -> {db_dt}")
                failures += 1
                continue
            try:
                csv_ts = parsed_csv_dt.astimezone(timezone('UTC')).timestamp()
                db_ts = db_dt.astimezone(timezone('UTC')).timestamp() if db_dt.tzinfo else db_dt.replace(tzinfo=timezone('UTC')).timestamp()
                if math.isclose(csv_ts, db_ts, rel_tol=0, abs_tol=1e-3):
                    print(f"✅ ID {csv_id} {col} OK: {parsed_csv_dt.isoformat()} == {db_dt.isoformat()}")
                else:
                    print(f"❌ ID {csv_id} {col} DIFFER: CSV -> {parsed_csv_dt.isoformat()}, DB -> {db_dt.isoformat()}")
                    failures += 1
            except Exception as e:
                print(f"⚠️  Erro ao comparar datetime para ID {csv_id} coluna {col}: {e}")
                failures += 1
    print(f"🔎 Validação concluída: {len(sample)} amostras verificadas, {failures} falhas detectadas")
    
# Função para inserir ou atualizar alerts a partir do CSV (UTF-16) - OTIMIZADO
def insert_or_update_alerts_from_csv(session, csv_file_path):
    """VERSÃO OTIMIZADA COM PANDAS - SÓ INSERT, SEM UPDATE LENTO"""
    start_time = time.time()
    print(f"🚀 PANDAS ULTRA (Alerts): {csv_file_path}")
    
    # 1. Ler CSV - PRESERVAR NULL
    df = pd.read_csv(csv_file_path, encoding='utf-16', skiprows=1, low_memory=False, keep_default_na=True, na_values=['', 'nan', 'NaN', 'N/A'])
    print(f"📁 Lido: {len(df)} registros em {time.time() - start_time:.2f}s")
    
    # 2. Renomear colunas conforme mapeamento
    df_columns = {col: ALERT_COLUMN_MAPPING[col] for col in df.columns if col in ALERT_COLUMN_MAPPING}
    df = df.rename(columns=df_columns)
    
    # 3. Garantir coluna id
    if 'id' not in df.columns:
        print("❌ Coluna 'id' não encontrada")
        return
    
    df = df.dropna(subset=['id'])
    df['id'] = df['id'].astype(str).str.strip()
    
    # 4. Processar datas
    for col in ['alert_at', 'status_changed_on', 'last_update', 'created_on']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: parse_datetime(str(x).strip()) if pd.notna(x) else None)
    
    # 5. Processar booleanos
    for col in ['is_smart', 'is_system_alert']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: True if str(x).strip().lower() == 'yes' else False if pd.notna(x) else None)
    
    # 6. PRESERVAR NULL - Não usar df.where() que pode converter NULL para 0 em colunas numéricas
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or (isinstance(x, str) and x.strip() == '') else x)
    
    print(f"🧹 Processado: {len(df)} registros")
    
    # 6. SÓ NOVOS (insert rápido)
    existing_ids = {row.id for row in session.query(Alert.id).all()}
    df_insert = df[~df['id'].isin(existing_ids)]
    
    if len(df_insert) > 0:
        df_insert = convert_nat_to_none(df_insert)
        session.bulk_insert_mappings(Alert, df_insert.to_dict('records'))
        session.commit()
        print(f"📥 Inseridos: {len(df_insert)} registros")
    else:
        print("📥 Nenhum registro novo")
    
    total_time = time.time() - start_time
    speed = len(df) / total_time if total_time > 0 else 0
    print(f"🎉 {total_time:.2f}s | ⚡ {speed:.0f} reg/s")


# Função para inserir ou atualizar alerts a partir do Excel - OTIMIZADO
def insert_or_update_alerts_from_excel(session, excel_file_path):
    """VERSÃO OTIMIZADA COM PANDAS - SÓ INSERT, SEM UPDATE LENTO"""
    start_time = time.time()
    print(f"🚀 PANDAS ULTRA (Alerts Excel): {excel_file_path}")
    
    # 1. Ler Excel - PRESERVAR NULL
    df = pd.read_excel(excel_file_path, engine='openpyxl', keep_default_na=True, na_values=['', 'nan', 'NaN', 'N/A'])
    print(f"📁 Lido: {len(df)} registros em {time.time() - start_time:.2f}s")
    
    # 2. Renomear colunas conforme mapeamento
    df_columns = {col: ALERT_COLUMN_MAPPING[col] for col in df.columns if col in ALERT_COLUMN_MAPPING}
    df = df.rename(columns=df_columns)
    
    # 3. Garantir coluna id
    if 'id' not in df.columns:
        print("❌ Coluna 'id' não encontrada")
        return
    
    df = df.dropna(subset=['id'])
    df['id'] = df['id'].astype(str).str.strip()
    
    # 4. Processar datas
    for col in ['alert_at', 'status_changed_on', 'last_update', 'created_on']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: parse_datetime(str(x).strip()) if pd.notna(x) else None)
    
    # 5. Processar booleanos
    for col in ['is_smart', 'is_system_alert']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: True if str(x).strip().lower() == 'yes' else False if pd.notna(x) else None)
    
    # 6. PRESERVAR NULL - Não usar df.where() que pode converter NULL para 0 em colunas numéricas
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or (isinstance(x, str) and x.strip() == '') else x)
    
    print(f"🧹 Processado: {len(df)} registros")
    
    # 6. SÓ NOVOS (insert rápido)
    existing_ids = {row.id for row in session.query(Alert.id).all()}
    df_insert = df[~df['id'].isin(existing_ids)]
    
    if len(df_insert) > 0:
        df_insert = convert_nat_to_none(df_insert)
        session.bulk_insert_mappings(Alert, df_insert.to_dict('records'))
        session.commit()
        print(f"📥 Inseridos: {len(df_insert)} registros")
    else:
        print("📥 Nenhum registro novo")
    
    total_time = time.time() - start_time
    speed = len(df) / total_time if total_time > 0 else 0
    print(f"🎉 {total_time:.2f}s | ⚡ {speed:.0f} reg/s")

# Função para inserir ou atualizar clientes a partir do CSV (UTF-16) - OTIMIZADO
def insert_or_update_clients_from_csv(session, csv_file_path):
    """VERSÃO OTIMIZADA COM PANDAS - SÓ INSERT, SEM UPDATE LENTO"""
    start_time = time.time()
    print(f"🚀 PANDAS ULTRA (Clients): {csv_file_path}")
    
    # 1. Ler CSV - PRESERVAR NULL
    df = pd.read_csv(csv_file_path, encoding='utf-16', skiprows=1, low_memory=False, keep_default_na=True, na_values=['', 'nan', 'NaN', 'N/A'])
    print(f"📁 Lido: {len(df)} registros em {time.time() - start_time:.2f}s")
    
    # 2. Renomear colunas conforme mapeamento
    df_columns = {col: CLIENT_COLUMN_MAPPING[col] for col in df.columns if col in CLIENT_COLUMN_MAPPING}
    df = df.rename(columns=df_columns)
    
    # 3. Garantir coluna id
    if 'id' not in df.columns:
        print("❌ Coluna 'id' não encontrada")
        return
    
    df = df.dropna(subset=['id'])
    df['id'] = df['id'].astype(str).str.strip()
    
    # 4. Processar datas
    for col in ['created_on', 'modified_on']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: parse_datetime(str(x).strip()) if pd.notna(x) else None)
    
    # 5. Processar booleanos
    bool_cols = ['is_feedback_enabled', 'vh_enabled', 'manual_processing_mode', 'is_visit_from_ping', 
                 'limit_location_distance', 'enable_pic_to_pog', 'disable_geo_data_collection']
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: True if str(x).strip().lower() == 'yes' else False if pd.notna(x) else None)
    
    # 6. Processar floats
    float_cols = ['vision_image_interval_hours', 'temperature_min', 'temperature_max', 
                  'cooler_tracking_displacement_threshold_mtr']
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 7. Processar integers
    int_cols = ['vision_image_interval_door_open', 'out_of_stock_sku', 'power_off_duration', 'light_min', 
                'light_max', 'door_count', 'health_intervals_hours', 'cooler_tracking_threshold_days', 
                'fallen_magnet_threshold', 'distance_in_meter', 'threshold_in_minutes', 'survey_distance']
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    # 8. PRESERVAR NULL - Não usar df.where() que pode converter NULL para 0 em colunas numéricas
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or (isinstance(x, str) and x.strip() == '') else x)
    
    print(f"🧹 Processado: {len(df)} registros")
    
    # 8. SÓ NOVOS (insert rápido)
    existing_ids = {row.id for row in session.query(Client.id).all()}
    df_insert = df[~df['id'].isin(existing_ids)]
    
    if len(df_insert) > 0:
        df_insert = convert_nat_to_none(df_insert)
        session.bulk_insert_mappings(Client, df_insert.to_dict('records'))
        session.commit()
        print(f"📥 Inseridos: {len(df_insert)} registros")
    else:
        print("📥 Nenhum registro novo")
    
    total_time = time.time() - start_time
    speed = len(df) / total_time if total_time > 0 else 0
    print(f"🎉 {total_time:.2f}s | ⚡ {speed:.0f} reg/s")


# Função para inserir ou atualizar clients a partir do Excel - OTIMIZADO
def insert_or_update_clients_from_excel(session, excel_file_path):
    """VERSÃO OTIMIZADA COM PANDAS - SÓ INSERT, SEM UPDATE LENTO"""
    start_time = time.time()
    print(f"🚀 PANDAS ULTRA (Clients Excel): {excel_file_path}")
    
    # 1. Ler Excel - PRESERVAR NULL
    df = pd.read_excel(excel_file_path, engine='openpyxl', keep_default_na=True, na_values=['', 'nan', 'NaN', 'N/A'])
    print(f"📁 Lido: {len(df)} registros em {time.time() - start_time:.2f}s")
    
    # 2. Renomear colunas conforme mapeamento
    df_columns = {col: CLIENT_COLUMN_MAPPING[col] for col in df.columns if col in CLIENT_COLUMN_MAPPING}
    df = df.rename(columns=df_columns)
    
    # 3. Garantir coluna id
    if 'id' not in df.columns:
        print("❌ Coluna 'id' não encontrada")
        return
    
    df = df.dropna(subset=['id'])
    df['id'] = df['id'].astype(str).str.strip()
    
    # 4. Processar datas
    for col in ['created_on', 'modified_on']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: parse_datetime(str(x).strip()) if pd.notna(x) else None)
    
    # 5. Processar booleanos
    bool_cols = ['is_feedback_enabled', 'vh_enabled', 'manual_processing_mode', 'is_visit_from_ping', 
                 'limit_location_distance', 'enable_pic_to_pog', 'disable_geo_data_collection']
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: True if str(x).strip().lower() == 'yes' else False if pd.notna(x) else None)
    
    # 6. Processar floats
    float_cols = ['vision_image_interval_hours', 'temperature_min', 'temperature_max', 
                  'cooler_tracking_displacement_threshold_mtr']
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 7. Processar integers
    int_cols = ['vision_image_interval_door_open', 'out_of_stock_sku', 'power_off_duration', 'light_min', 
                'light_max', 'door_count', 'health_intervals_hours', 'cooler_tracking_threshold_days', 
                'fallen_magnet_threshold', 'distance_in_meter', 'threshold_in_minutes', 'survey_distance']
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    # 8. PRESERVAR NULL - Não usar df.where() que pode converter NULL para 0 em colunas numéricas
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or (isinstance(x, str) and x.strip() == '') else x)
    
    print(f"🧹 Processado: {len(df)} registros")
    
    # 8. SÓ NOVOS (insert rápido)
    existing_ids = {row.id for row in session.query(Client.id).all()}
    df_insert = df[~df['id'].isin(existing_ids)]
    
    if len(df_insert) > 0:
        df_insert = convert_nat_to_none(df_insert)
        session.bulk_insert_mappings(Client, df_insert.to_dict('records'))
        session.commit()
        print(f"📥 Inseridos: {len(df_insert)} registros")
    else:
        print("📥 Nenhum registro novo")
    
    total_time = time.time() - start_time
    speed = len(df) / total_time if total_time > 0 else 0
    print(f"🎉 {total_time:.2f}s | ⚡ {speed:.0f} reg/s")

# Função para inserir ou atualizar subclientes a partir do CSV (UTF-16) - OTIMIZADO
def insert_or_update_subclients_from_csv(session, csv_file_path):
    """VERSÃO OTIMIZADA COM PANDAS - SÓ INSERT, SEM UPDATE LENTO"""
    start_time = time.time()
    print(f"🚀 PANDAS ULTRA (SubClients): {csv_file_path}")
    
    # 1. Ler CSV - PRESERVAR NULL
    df = pd.read_csv(csv_file_path, encoding='utf-16', skiprows=1, low_memory=False, keep_default_na=True, na_values=['', 'nan', 'NaN', 'N/A'])
    print(f"📁 Lido: {len(df)} registros em {time.time() - start_time:.2f}s")
    
    # 2. Renomear colunas conforme mapeamento
    df_columns = {col: SUBCLIENT_COLUMN_MAPPING[col] for col in df.columns if col in SUBCLIENT_COLUMN_MAPPING}
    df = df.rename(columns=df_columns)
    
    # 3. Verificar colunas necessárias
    if 'subclient_code' not in df.columns or 'client' not in df.columns:
        print("❌ Colunas 'subclient_code' ou 'client' não encontradas")
        return
    
    df = df.dropna(subset=['subclient_code', 'client'])
    
    # 4. Gerar ID único (subclient_code + client)
    df['id'] = df['subclient_code'].astype(str).str.strip() + '_' + df['client'].astype(str).str.strip()
    
    # 5. Limpar strings
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: str(x).strip() if pd.notna(x) else None)
    
    # 6. PRESERVAR NULL - Não usar df.where() que pode converter NULL para 0 em colunas numéricas
    # Já feito acima no loop de limpeza de strings
    
    print(f"🧹 Processado: {len(df)} registros")
    
    # 6. SÓ NOVOS (insert rápido)
    existing_ids = {row.id for row in session.query(SubClient.id).all()}
    df_insert = df[~df['id'].isin(existing_ids)]
    
    if len(df_insert) > 0:
        df_insert = convert_nat_to_none(df_insert)
        session.bulk_insert_mappings(SubClient, df_insert.to_dict('records'))
        session.commit()
        print(f"📥 Inseridos: {len(df_insert)} registros")
    else:
        print("📥 Nenhum registro novo")
    
    total_time = time.time() - start_time
    speed = len(df) / total_time if total_time > 0 else 0
    print(f"🎉 {total_time:.2f}s | ⚡ {speed:.0f} reg/s")


# ============================================================================
# GENERIC FUNCTIONS - Support for both Excel and CSV files
# ============================================================================

def get_file_extension(file_path):
    """Get file extension"""
    return Path(file_path).suffix.lower()


def is_csv_file(file_path):
    """Check if file is CSV"""
    return get_file_extension(file_path) == '.csv'


def is_excel_file(file_path):
    """Check if file is Excel"""
    return get_file_extension(file_path) in ['.xlsx', '.xls']


def insert_or_update_users(session, file_path):
    """
    Universal function to import users from either Excel or CSV
    """
    if is_excel_file(file_path):
        return insert_or_update_users_from_excel(session, file_path)
    else:
        print(f"⚠️  CSV format not implemented for Users. Please use Excel file (.xlsx)")
        return None


def insert_or_update_outlets(session, file_path):
    """
    Universal function to import outlets from either Excel or CSV
    """
    if is_excel_file(file_path):
        return insert_or_update_outlets_from_excel(session, file_path)
    else:
        print(f"⚠️  CSV format not implemented for Outlets. Please use Excel file (.xlsx)")
        return None


def insert_or_update_assets(session, file_path):
    """
    Universal function to import assets from either Excel or CSV
    """
    if is_excel_file(file_path):
        return insert_or_update_assets_from_excel(session, file_path)
    else:
        print(f"⚠️  CSV format not implemented for Assets. Please use Excel file (.xlsx)")
        return None


def insert_or_update_smartdevices(session, file_path):
    """
    Universal function to import smart devices from either Excel or CSV
    """
    if is_excel_file(file_path):
        return insert_or_update_smartdevices_from_excel(session, file_path)
    else:
        print(f"⚠️  CSV format not implemented for SmartDevices. Please use Excel file (.xlsx)")
        return None


def insert_or_update_movements(session, file_path):
    """
    Universal function to import movements from either Excel or CSV
    """
    if is_excel_file(file_path):
        return insert_or_update_movements_from_excel(session, file_path)
    else:
        print(f"⚠️  CSV format not implemented for Movements. Please use Excel file (.xlsx)")
        return None


def insert_or_update_health_events(session, file_path):
    """
    Universal function to import health events from either Excel or CSV (UTF-16)
    """
    if is_csv_file(file_path):
        return insert_or_update_health_events_from_csv(session, file_path)
    elif is_excel_file(file_path):
        return insert_or_update_health_events_from_excel(session, file_path)
    else:
        print(f"❌ Unsupported file format: {file_path}")
        return None


def insert_or_update_door_events(session, file_path):
    """
    Universal function to import door events from either Excel or CSV (UTF-16)
    """
    if is_csv_file(file_path):
        return insert_or_update_door_events_from_csv(session, file_path)
    elif is_excel_file(file_path):
        print(f"⚠️  Excel format for Door Events may lose large IDs. CSV (UTF-16) is recommended.")
        # Could implement Excel support here if needed
        return None
    else:
        print(f"❌ Unsupported file format: {file_path}")
        return None


def insert_or_update_alerts(session, file_path):
    """
    Universal function to import alerts from either Excel or CSV (UTF-16)
    """
    if is_csv_file(file_path):
        return insert_or_update_alerts_from_csv(session, file_path)
    elif is_excel_file(file_path):
        # Support Excel format as well (with warning about IDs)
        print(f"⚠️  Excel format for Alerts may lose large IDs. CSV (UTF-16) is recommended.")
        return insert_or_update_alerts_from_excel(session, file_path)
    else:
        print(f"❌ Unsupported file format: {file_path}")
        return None


def insert_or_update_clients(session, file_path):
    """
    Universal function to import clients from either Excel or CSV (UTF-16)
    """
    if is_csv_file(file_path):
        return insert_or_update_clients_from_csv(session, file_path)
    elif is_excel_file(file_path):
        print(f"⚠️  Excel format for Clients may lose large data. CSV (UTF-16) is recommended.")
        return insert_or_update_clients_from_excel(session, file_path)
    else:
        print(f"❌ Unsupported file format: {file_path}")
        return None


def insert_or_update_subclients(session, file_path):
    """
    Universal function to import subclients from either Excel or CSV (UTF-16)
    """
    if is_csv_file(file_path):
        return insert_or_update_subclients_from_csv(session, file_path)
    elif is_excel_file(file_path):
        print(f"⚠️  Excel format for SubClients. CSV (UTF-16) is recommended.")
        # Could implement Excel support here if needed
        return None
    else:
        print(f"❌ Unsupported file format: {file_path}")
        return None


# Função para inserir ou atualizar alerts definition a partir do Excel - OTIMIZADO
def insert_or_update_alerts_definition_from_excel(session, excel_file_path):
    """VERSÃO OTIMIZADA COM PANDAS - SÓ INSERT, SEM UPDATE LENTO"""
    start_time = time.time()
    print(f"🚀 PANDAS ULTRA (AlertsDefinition): {excel_file_path}")
    
    # 1. Ler Excel - PRESERVAR NULL
    df = pd.read_excel(excel_file_path, engine='openpyxl', keep_default_na=True, na_values=['', 'nan', 'NaN', 'N/A'])
    print(f"📁 Lido: {len(df)} registros em {time.time() - start_time:.2f}s")
    
    # 2. Renomear colunas conforme mapeamento
    df_columns = {col: ALERTS_DEFINITION_COLUMN_MAPPING[col] for col in df.columns if col in ALERTS_DEFINITION_COLUMN_MAPPING}
    df = df.rename(columns=df_columns)
    
    # 3. Garantir coluna name
    if 'name' not in df.columns:
        print("❌ Coluna 'name' não encontrada")
        return
    
    df = df.dropna(subset=['name'])
    df['name'] = df['name'].astype(str).str.strip()
    
    # 4. Processar datas
    for col in ['created_on', 'modified_on']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: parse_datetime(str(x).strip()) if pd.notna(x) else None)
    
    # 5. Processar integers
    int_cols = ['open_alert', 'updated_alert', 'movement_detected', 'power_off_duration', 
                'temperature_below', 'temperature_above', 'offline_alert_time', 'online_alert_time',
                'missing_faulty_time', 'cooler_disconnect_threshold', 'alert_age_threshold',
                'prolonged_irregularity_min', 'no_data_threshold', 'battery_open_threshold',
                'battery_close_threshold', 'stock_threshold', 'purity_threshold', 'planogram_threshold',
                'gps_displacement_threshold', 'motion_available_time', 'par_displacement_meter',
                'colas_threshold', 'flavours_threshold', 'colas_flavours', 'lane_threshold', 'min_stock']
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    # 6. PRESERVAR NULL - Não usar df.where() que pode converter NULL para 0 em colunas numéricas
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or (isinstance(x, str) and x.strip() == '') else x)
    
    print(f"🧹 Processado: {len(df)} registros")
    
    # 6. SÓ NOVOS (insert rápido)
    existing_names = {row.name for row in session.query(AlertsDefinition.name).all()}
    df_insert = df[~df['name'].isin(existing_names)]
    
    if len(df_insert) > 0:
        df_insert = convert_nat_to_none(df_insert)
        session.bulk_insert_mappings(AlertsDefinition, df_insert.to_dict('records'))
        session.commit()
        print(f"📥 Inseridos: {len(df_insert)} registros")
    else:
        print("📥 Nenhum registro novo")
    
    total_time = time.time() - start_time
    speed = len(df) / total_time if total_time > 0 else 0
    print(f"🎉 {total_time:.2f}s | ⚡ {speed:.0f} reg/s")


# Função para inserir ou atualizar alerts definition a partir do CSV (UTF-16) - OTIMIZADO
def insert_or_update_alerts_definition_from_csv(session, csv_file_path):
    """VERSÃO OTIMIZADA COM PANDAS - SÓ INSERT, SEM UPDATE LENTO"""
    start_time = time.time()
    print(f"🚀 PANDAS ULTRA (AlertsDefinition CSV): {csv_file_path}")
    
    # 1. Ler CSV - PRESERVAR NULL
    df = pd.read_csv(csv_file_path, encoding='utf-16', skiprows=1, low_memory=False, keep_default_na=True, na_values=['', 'nan', 'NaN', 'N/A'])
    print(f"📁 Lido: {len(df)} registros em {time.time() - start_time:.2f}s")
    
    # 2. Renomear colunas conforme mapeamento
    df_columns = {col: ALERTS_DEFINITION_COLUMN_MAPPING[col] for col in df.columns if col in ALERTS_DEFINITION_COLUMN_MAPPING}
    df = df.rename(columns=df_columns)
    
    # 3. Garantir coluna name
    if 'name' not in df.columns:
        print("❌ Coluna 'name' não encontrada")
        return
    
    df = df.dropna(subset=['name'])
    df['name'] = df['name'].astype(str).str.strip()
    
    # 4. Processar datas
    for col in ['created_on', 'modified_on']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: parse_datetime(str(x).strip()) if pd.notna(x) else None)
    
    # 5. Processar integers
    int_cols = ['open_alert', 'updated_alert', 'movement_detected', 'power_off_duration', 
                'temperature_below', 'temperature_above', 'offline_alert_time', 'online_alert_time',
                'missing_faulty_time', 'cooler_disconnect_threshold', 'alert_age_threshold',
                'prolonged_irregularity_min', 'no_data_threshold', 'battery_open_threshold',
                'battery_close_threshold', 'stock_threshold', 'purity_threshold', 'planogram_threshold',
                'gps_displacement_threshold', 'motion_available_time', 'par_displacement_meter',
                'colas_threshold', 'flavours_threshold', 'colas_flavours', 'lane_threshold', 'min_stock']
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    # 6. PRESERVAR NULL - Não usar df.where() que pode converter NULL para 0 em colunas numéricas
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: None if pd.isna(x) or (isinstance(x, str) and x.strip() == '') else x)
    
    print(f"🧹 Processado: {len(df)} registros")
    
    # 6. SÓ NOVOS (insert rápido)
    existing_names = {row.name for row in session.query(AlertsDefinition.name).all()}
    df_insert = df[~df['name'].isin(existing_names)]
    
    if len(df_insert) > 0:
        df_insert = convert_nat_to_none(df_insert)
        session.bulk_insert_mappings(AlertsDefinition, df_insert.to_dict('records'))
        session.commit()
        print(f"📥 Inseridos: {len(df_insert)} registros")
    else:
        print("📥 Nenhum registro novo")
    
    total_time = time.time() - start_time
    speed = len(df) / total_time if total_time > 0 else 0
    print(f"🎉 {total_time:.2f}s | ⚡ {speed:.0f} reg/s")

def insert_or_update_alerts_definition(session, file_path):
    """
    Auto-detect file format and import alerts definition data.
    """
    if is_excel_file(file_path):
        return insert_or_update_alerts_definition_from_excel(session, file_path)
    elif is_csv_file(file_path):
        return insert_or_update_alerts_definition_from_csv(session, file_path)
    else:
        print(f"❌ Unsupported file format for alerts definition: {file_path}")
        return None


def import_all_from_directory(session, directory_path, verbose=True):
    """
    Auto-detect and import all supported files from a directory.
    
    Expects files with standard names:
    - users.xlsx
    - outlets.xlsx
    - assets.xlsx
    - smartdevices.xlsx
    - movements.xlsx
    - health_events.xlsx
    - door_events.csv (UTF-16)
    - alerts.csv (UTF-16)
    - clients.csv (UTF-16)
    - subclients.csv (UTF-16)
    """
    if not os.path.isdir(directory_path):
        print(f"❌ Directory not found: {directory_path}")
        return False
    
    print(f"\n🔍 Scanning directory: {directory_path}")
    print("=" * 70)
    
    imported_count = 0
    skipped_count = 0
    
    # Define file mappings: (expected_name_pattern, import_function)
    file_mappings = [
        ('users', insert_or_update_users),
        ('outlets', insert_or_update_outlets),
        ('assets', insert_or_update_assets),
        ('smartdevices', insert_or_update_smartdevices),
        ('movements', insert_or_update_movements),
        ('health_events', insert_or_update_health_events),
        ('door_events', insert_or_update_door_events),
        ('alerts_definition', insert_or_update_alerts_definition),
        ('alert_definition', insert_or_update_alerts_definition),
        ('alerts', insert_or_update_alerts),
        ('clients', insert_or_update_clients),
        ('subclients', insert_or_update_subclients),
    ]
    
    import re
    for file_name in sorted(os.listdir(directory_path)):
        if file_name.startswith('.'):
            continue
            
        file_path = os.path.join(directory_path, file_name)
        file_base = Path(file_name).stem.lower()
        # Normalize filename: create two variants
        # - sanitized_base: underscores for readability
        sanitized_base = re.sub(r'[^a-z0-9]+', '_', file_base)
        # - normalized_base: remove all non-alphanumeric chars so 'smart_devices' and 'smartdevices' match
        normalized_base = re.sub(r'[^a-z0-9]+', '', file_base)
        
        # Find matching import function
        for pattern, import_func in file_mappings:
            # Normalize pattern as well to support patterns with underscores
            pattern_sanitized = re.sub(r'[^a-z0-9]+', '_', pattern)
            pattern_normalized = re.sub(r'[^a-z0-9]+', '', pattern)
            if pattern in file_base or pattern in sanitized_base or pattern_sanitized in sanitized_base or pattern_normalized in normalized_base:
                if is_excel_file(file_path) or is_csv_file(file_path):
                    try:
                        if verbose:
                            print(f"📥 Importing: {file_name}")
                        import_func(session, file_path)
                        print(f"✅ {file_name} imported successfully")
                        imported_count += 1
                    except Exception as e:
                        print(f"❌ Error importing {file_name}: {str(e)}")
                        skipped_count += 1
                else:
                    if verbose:
                        print(f"⏭️  Skipping: {file_name} (unsupported format)")
                    skipped_count += 1
                break
    
    print("=" * 70)
    print(f"✨ Import completed: {imported_count} files imported, {skipped_count} skipped")
    return True