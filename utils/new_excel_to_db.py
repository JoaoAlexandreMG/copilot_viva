import pandas as pd # Novo e principal import

import openpyxl

import datetime

import warnings

import os

import csv # Novo import para CSV

from pytz import timezone as pytz_timezone # Necessário para a lógica de TZ

from dateutil import parser

from dateutil.parser import UnknownTimezoneWarning

from datetime import datetime, timedelta, timezone

# Importe todos os modelos necessários e a função de sessão

from db.database import get_session

from models.models import HealthEvent, User, Outlet, Asset, SmartDevice, Movement, DoorEvent, Alert, Client, SubClient, AlertsDefinition



# ============================================================================

# ⚙️ CONFIGURAÇÕES DE DADOS E MAPEAMENTOS

# ============================================================================



# ⚠️ Ignora o warning do dateutil, pois estamos tratando as abreviações explicitamente

warnings.filterwarnings("ignore", category=UnknownTimezoneWarning)



# Define os offsets de fuso horário para conversão correta para UTC

TZ_INFOS = {

    'BRST': timezone(timedelta(hours=-2)),  # UTC-02:00

    'ESAST': timezone(timedelta(hours=-2)), # UTC-02:00

    'BRT': timezone(timedelta(hours=-3)),   # UTC-03:00 (Incluído por segurança)

    'UTC': timezone.utc

}



# --- MAPEAMENTOS DE COLUNAS ---

# Seus mapeamentos originais completos (Apenas HealthEvent e User mostrados para brevidade)



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

    "Day": "day",

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

    "Day": "day",

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

    "Day": "day",

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

    "Day": "day",

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





# --- REGRAS DE PROCESSAMENTO POR MODELO ---

DATE_COLUMNS = [

    # User

    "last_login_on",

    "created_on",

    "modified_on",



    # Asset

    "asset_ping",

    "last_scan",

    "latest_health_record_event_time",

    "asset_associated_on",

    "gateway_associated_on",

    "acquisition_date",

    "latest_movement_record_event_time",

    "latest_power_record_event_time",

    "last_location_status_on",

    "latest_location_status_on",



    # SmartDevice

    "last_ping",

    "last_sim_status_updated_on",



    # Movement

    "start_time",

    "end_time",



    # HealthEvent

    "event_time",



    # DoorEvent

    "open_event_time",

    "close_event_time",



    # Alert

    "alert_at",

    "status_changed_on",

    "last_update",



    # Client

    # (created_on, modified_on already included above)



    # AlertsDefinition

    # (created_on, modified_on already included above)

]



BOOLEAN_COLUMNS = [

    # User

    "is_active",



    # Outlet

    "is_key_outlet",

    "is_smart",



    # Asset

    "is_competition",

    "is_factory_asset",

    "associated_in_factory",

    "prime_position",

    "is_missing",

    "is_vision",

    "is_authorized_movement",

    "is_unhealthy",

    "is_power_on",

    "has_sim",



    # SmartDevice

    "plugin_connected_ffxy",

    "is_device_registered_in_iot_hub",

    "is_sd_gateway",



    # Movement

    "door_open",



    # Alert

    "is_system_alert",



    # Client

    "is_feedback_enabled",

    "vh_enabled",

    "manual_processing_mode",

    "is_visit_from_ping",

    "limit_location_distance",

    "enable_pic_to_pog",

    "disable_geo_data_collection"

]



MODEL_RULES = {

    "HealthEvent": {

        "model": HealthEvent,

        "mapping": HEALTH_EVENT_COLUMN_MAPPING,

        "key_col": "id",

        "special_sanitizers": {}

    },

    "User": {

        "model": User,

        "mapping": USER_COLUMN_MAPPING,

        "key_col": "upn",

        "special_sanitizers": {}

    },

    "Outlet": {

        "model": Outlet,

        "mapping": OUTLET_COLUMN_MAPPING,

        "key_col": "code",

        "special_sanitizers": {}

    },

    "Asset": {

        "model": Asset,

        "mapping": ASSET_COLUMN_MAPPING,

        "key_col": "oem_serial_number",

        "special_sanitizers": {}

    },

    "SmartDevice": {

        "model": SmartDevice,

        "mapping": SMARTDEVICE_COLUMN_MAPPING,

        "key_col": "mac_address",

        "special_sanitizers": {}

    },

    "Movement": {

        "model": Movement,

        "mapping": MOVEMENT_COLUMN_MAPPING,

        "key_col": "id",

        "special_sanitizers": {}

    },

    "DoorEvent": {

        "model": DoorEvent,

        "mapping": DOOR_EVENT_COLUMN_MAPPING,

        "key_col": "id",

        "special_sanitizers": {}

    },

    "Alert": {

        "model": Alert,

        "mapping": ALERT_COLUMN_MAPPING,

        "key_col": "id",

        "special_sanitizers": {}

    },

    "Client": {

        "model": Client,

        "mapping": CLIENT_COLUMN_MAPPING,

        "key_col": "client_code",

        "special_sanitizers": {}

    },

    "SubClient": {

        "model": SubClient,

        "mapping": SUBCLIENT_COLUMN_MAPPING,

        "key_col": "subclient_code",

        "special_sanitizers": {}

    },

    "AlertsDefinition": {
        "model": AlertsDefinition,
        "mapping": ALERTS_DEFINITION_COLUMN_MAPPING,
        "key_col": ["name", "client"],
        "special_sanitizers": {}
    }

}



# ============================================================================

# 🧱 FUNÇÕES DE BASE (Base Functions)

# ============================================================================



def convert_excel_datetime_to_utc(date_string):

    """

    Função genérica de conversão de data/hora para UTC, usando tzinfos.

    """

    if not isinstance(date_string, str) or date_string is None:

        if isinstance(date_string, datetime):

            if date_string.tzinfo is None:

                local_tz = pytz_timezone("America/Sao_Paulo")

                return local_tz.localize(date_string).astimezone(timezone.utc)

            return date_string.astimezone(timezone.utc)

        return date_string

   

   

    try:

        dt_aware = parser.parse(

            date_string,

            dayfirst=True,

            ignoretz=False,

            tzinfos=TZ_INFOS

        )

        return dt_aware.astimezone(timezone.utc)



    except Exception:

        return None



# ----------------------------------------------------------------------



## ⚙️ Função Genérica de Importação (Agora suporta CSV e Excel)



def importar_dados_generico(db_session, model_name: str, file_path: str):

    """

    Importa e atualiza dados de um arquivo Excel ou CSV de forma genérica.

    """

    if model_name not in MODEL_RULES:

        print(f"❌ Regras de importação não encontradas para o modelo: {model_name}")

        return None



    rules = MODEL_RULES[model_name]

    ModelClass = rules['model']

    mapping = rules['mapping']

    key_col = rules['key_col']



    print(f"🚀 Iniciando importação para {model_name}...")

   

    file_ext = os.path.splitext(file_path)[1].lower()

    headers = []

    records_raw = []



    # --- Lógica de Leitura (Detecção de Formato) ---

    try:

        if file_ext in ['.xlsx', '.xls']:

            wb = openpyxl.load_workbook(file_path, read_only=True)  # read_only para performance

            sheet = wb.active

            headers = [cell.value for cell in sheet[1]]

            records_raw = [row for row in sheet.iter_rows(min_row=2, values_only=True)]

            print(f"📊 Lidos {len(records_raw)} registros do Excel")

       

        elif file_ext == '.csv':

            # Tenta UTF-16 primeiro (mais comum nos arquivos do portal), depois UTF-8
            encodings_to_try = ['utf-16', 'utf-8', 'latin-1']
            records_raw = None
            headers = None
            
            for enc in encodings_to_try:
                try:
                    with open(file_path, 'r', newline='', encoding=enc) as f:
                        reader = csv.DictReader(f)
                        headers_orig = list(reader.fieldnames) if reader.fieldnames else []
                        
                        if not headers_orig:
                            continue
                        
                        # Se primeiro header é um título único, pula e relê
                        if len(headers_orig) == 1:
                            with open(file_path, 'r', newline='', encoding=enc) as f:
                                f.readline()  # Pula linha 1
                                reader = csv.DictReader(f)
                                headers = list(reader.fieldnames) if reader.fieldnames else []
                                records_raw_dicts = list(reader)
                        else:
                            headers = headers_orig
                            records_raw_dicts = list(reader)
                        
                        # Converte de dicts para listas na ordem dos headers
                        records_raw = []
                        for row_dict in records_raw_dicts:
                            row = [row_dict.get(h) for h in headers]
                            records_raw.append(row)
                        break  # Sucesso, sai do loop
                        
                except (UnicodeDecodeError, Exception):
                    if enc == encodings_to_try[-1]:  # Última tentativa falhou
                        raise
                    continue  # Tenta próximo encoding
            
            if records_raw is None:
                raise ValueError(f"Não foi possível ler o arquivo CSV: {file_path}")

            # Sanitiza dados do CSV (strings vazias para None)

            # O CSV lê tudo como string, precisamos converter strings vazias para None para serem tratadas.

            records_raw = [[None if (isinstance(cell, str) and cell.strip() == '') else cell for cell in row] for row in records_raw]

        else:

            print(f"❌ Formato de arquivo não suportado: {file_path}")

            return None

           

        # Garante que os headers são strings limpas

        headers = [h.strip() if isinstance(h, str) else h for h in headers]

       

    except Exception as e:

        print(f"❌ Erro ao ler o arquivo {file_path}: {e}")

        return None

    # --- Fim Lógica de Leitura ---





    # 🚀 OTIMIZAÇÃO: Processamento em lote mais eficiente
    print(f"⚙️ Processando {len(records_raw)} registros...")
    records_to_process = []
    
    # Cria mapa de headers uma única vez para otimizar acesso
    header_mapping = {}
    for i, header in enumerate(headers):
        if header in mapping:
            header_mapping[i] = mapping[header]

    for row_raw in records_raw:
        # Ignora linhas vazias rapidamente
        if not any(row_raw[:2]):  # Verifica apenas primeiras 2 colunas
            continue

        data = {}
        
        # Usa mapeamento pré-computado para acesso direto
        for i, mapped_key in header_mapping.items():
            if i >= len(row_raw):
                continue
                
            value = row_raw[i]
            
            # --- 1. Tratamento de N/A e Nulls (otimizado) ---
            if isinstance(value, str):
                value_upper = value.strip().upper()
                if value_upper in ["N/A", "NONE", "NULL", ""]:
                    value = None
            
            # --- 2. Tratamento Específico ---
            if mapped_key in rules['special_sanitizers']:
                sanitizer_func = rules['special_sanitizers'][mapped_key]
                value = sanitizer_func(value)
           
            # --- 3. Tratamento de Data/Hora ---
            if mapped_key in DATE_COLUMNS and value is not None:
                value = convert_excel_datetime_to_utc(value)
           
            # --- 4. Tratamento de Boolean (otimizado) ---
            if mapped_key in BOOLEAN_COLUMNS and isinstance(value, str):
                value_lower = value.strip().lower()
                if value_lower == "yes":
                    value = True
                elif value_lower == "no":
                    value = False
                       
            data[mapped_key] = value

        if data:
            records_to_process.append(data)

    print(f"✅ Processados {len(records_to_process)} registros válidos")

    # 🚀 DEDUPLICAÇÃO: Remove duplicatas dentro do arquivo pela chave primária
    print(f"🔄 Deduplicando registros por chave primária...")
    deduplicated = {}
    for data in records_to_process:
        if isinstance(key_col, list):
            key_tuple = tuple(data.get(col) for col in key_col)
            if all(v is not None for v in key_tuple):
                deduplicated[key_tuple] = data
        else:
            key = data.get(key_col)
            if key is not None:
                deduplicated[key] = data
    
    records_to_process = list(deduplicated.values())
    print(f"📊 Após deduplicação: {len(records_to_process)} registros únicos")






    inserted_count = 0
    updated_count = 0

    # Lógica de Persistência (INSERT/UPDATE)
    for data in records_to_process:
        # Suporte para chaves compostas
        if isinstance(key_col, list):
            # Chave composta (ex: name + client)
            key_values = {col: data.get(col) for col in key_col}
            if any(v is None for v in key_values.values()):
                continue
            
            # Constrói filtro para chave composta
            filters = [getattr(ModelClass, col) == value for col, value in key_values.items()]
            existing_record = db_session.query(ModelClass).filter(*filters).first()
        else:
            # Chave simples (comportamento original)
            event_key = data.get(key_col)
            if event_key is None:
                continue
            existing_record = db_session.query(ModelClass).filter(getattr(ModelClass, key_col) == event_key).first()

        if existing_record:
            for key, value in data.items():
                if value is not None:
                    setattr(existing_record, key, value)
            updated_count += 1
        else:
            new_record = ModelClass(**data)
            db_session.add(new_record)
            inserted_count += 1

           

    # Commit Único
    try:
        db_session.commit()
        print(f"✅ {model_name} importado com sucesso. Inseridos: {inserted_count}, Atualizados: {updated_count}.")
       
        # Refresh Materialized Views para tabelas que afetam as views
        tables_requiring_mv_refresh = ["Movement", "HealthEvent", "DoorEvent", "Asset", "Alert", "SmartDevice"]
        if model_name in tables_requiring_mv_refresh:
            print(f"🔄 Atualizando Materialized Views devido a mudanças em {model_name}...")
            try:
                from sqlalchemy import text
                db_session.execute(text("REFRESH MATERIALIZED VIEW mv_client_assets_report;"))
                db_session.execute(text("REFRESH MATERIALIZED VIEW mv_dashboard_hourly_metrics;"))
                db_session.execute(text("REFRESH MATERIALIZED VIEW mv_dashboard_stats_main;"))
                db_session.commit()
                print("✅ Materialized Views atualizadas com sucesso.")
            except Exception as mv_error:
                print(f"⚠️ Erro ao atualizar Materialized Views: {mv_error}")
                # Não faz rollback pois os dados já foram commitados com sucesso
       
        # Retorna informações sobre a importação
        return {
            'inserted': inserted_count,
            'updated': updated_count,
            'model': model_name
        }
       
    except Exception as e:
        db_session.rollback()
        print(f"❌ Erro ao comitar transação para {model_name}: {e}")
        raise

       

# ----------------------------------------------------------------------



if __name__ == "__main__":

    # É necessário que você tenha a função get_session() configurada.

    # from db.database import get_session

    db_session = get_session()

   

    # Exemplo: Importar HealthEvents de um arquivo CSV

    importar_dados_generico(db_session, "AlertsDefinition", "docs/Alert Definition11.16.25 04.53.XLSX")

   

    # print("Script de importação genérico configurado. Adicionado suporte a CSV.")