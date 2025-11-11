import requests
import psycopg2
import pandas as pd
from datetime import datetime, date
from typing import Optional
import os
from dateutil import parser as date_parser
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()


def _ensure_datetime(value: Optional[datetime], field_name: str) -> datetime:
    """
    Normaliza entradas de data aceitando datetime ou string e retorna datetime.
    """
    if value is None:
        raise ValueError(f"Valor de {field_name} nao pode ser None")

    if isinstance(value, datetime):
        return value

    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())

    if isinstance(value, str):
        value = value.strip()
        if not value:
            raise ValueError(f"Valor de {field_name} vazio")
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    raise ValueError(f"Formato de data invalido para {field_name}: {value}")


def _parse_bool(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        try:
            return bool(int(value))
        except (TypeError, ValueError):
            return None
    if isinstance(value, str):
        cleaned = value.strip().lower()
        if not cleaned:
            return None
        if cleaned in {"yes", "y", "true", "t", "sim", "s", "1"}:
            return True
        if cleaned in {"no", "n", "false", "f", "nao", "0"}:
            return False
    return None


def _parse_float(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", ".")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _parse_datetime(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        try:
            return date_parser.parse(cleaned, dayfirst=True, fuzzy=True, ignoretz=True)
        except (ValueError, TypeError):
            return None
    return None


def logar():
    # Criar uma sessão para manter cookies
    session = requests.Session()

    url = "https://portal.visioniot.net/login.aspx?ReturnUrl=%2fdefault.aspx"

    print("Passo 1: Fazendo GET inicial para estabelecer sessao...")
    # Fazer GET primeiro para obter cookies de sessão
    response_get = session.get(url)
    print(f"GET Status: {response_get.status_code}")

    # Headers para o POST de login (igual ao navegador)
    headers_login = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://portal.visioniot.net',
        'Referer': 'https://portal.visioniot.net/login.aspx?ReturnUrl=%2fdefault.aspx',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0'
    }

    # Payload simples como no navegador
    payload_login = {
        'Username': 'PBI_VIVA',
        'Password': '#Ab01020304',
        'timezone': 'Brasilia Standard Time'
    }

    print("\nPasso 2: Fazendo POST com credenciais...")
    # Fazer POST de login (allow_redirects=False para detectar redirect)
    response_post = session.post(url, data=payload_login, headers=headers_login, allow_redirects=False)

    print(f"Login Status: {response_post.status_code}")

    # Status 302 ou Location header = sucesso com redirect
    if response_post.status_code == 302 or 'Location' in response_post.headers:
        print("OK - Login bem-sucedido! Seguindo redirect...")

        # Seguir o redirect manualmente
        redirect_url = response_post.headers.get('Location', '/default.aspx')
        if not redirect_url.startswith('http'):
            redirect_url = f"https://portal.visioniot.net{redirect_url}"

        response_default = session.get(redirect_url)
        print(f"Default Page Status: {response_default.status_code}")

        return session

    # Status 401 = sessão existente, precisa terminar
    elif response_post.status_code == 401:
        print("Sessao existente detectada. Terminando sessao anterior...")

        # Headers AJAX para terminar sessão existente
        headers_ajax = {
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://portal.visioniot.net/login.aspx?ReturnUrl=%2fdefault.aspx',
            'Origin': 'https://portal.visioniot.net',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0'
        }

        payload_terminate = {
            'action': 'AllowLoginAndTerminateExistingSession',
            'Username': 'PBI_VIVA',
            'Password': '#Ab01020304'
        }


        response_terminate = session.post(url, data=payload_terminate, headers=headers_ajax)
        print(f"Terminate Session Status: {response_terminate.status_code}")

        if response_terminate.status_code == 200:
            print("OK - Sessao anterior terminada!")

            # Verificar cookies após terminar sessão
            print("Cookies apos terminar sessao:")
            for cookie in session.cookies:
                print(f"  {cookie.name}")

            # IMPORTANTE: Após terminar a sessão, fazer login normal novamente
            print("\nFazendo login apos terminar sessao existente...")
            response_post2 = session.post(url, data=payload_login, headers=headers_login, allow_redirects=False)
            print(f"Login Status (2a tentativa): {response_post2.status_code}")

            if response_post2.status_code == 302 or 'Location' in response_post2.headers:
                redirect_url = response_post2.headers.get('Location', '/default.aspx')
                if not redirect_url.startswith('http'):
                    redirect_url = f"https://portal.visioniot.net{redirect_url}"

                response_default = session.get(redirect_url)
                print(f"Default Page Status: {response_default.status_code}")
                print("OK - Login completo apos terminar sessao!")

                # Verificar cookies finais
                print("Cookies finais:")
                for cookie in session.cookies:
                    print(f"  {cookie.name}")

                return session
            else:
                print("ERRO - Falha no login apos terminar sessao")
                # Acessar default.aspx mesmo assim
                response_default = session.get('https://portal.visioniot.net/default.aspx')
                print(f"Default Page Status: {response_default.status_code}")
                return session
        else:
            print(f"ERRO - Falha ao terminar sessao: {response_terminate.status_code}")
            return session

    else:
        print(f"ERRO - Falha no login: {response_post.status_code}")
        print(f"Resposta: {response_post.text[:500]}")
        return session

def buscar_registros_saude(session, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
    """
    Busca e exporta registros de saúde dos dispositivos (SmartDeviceHealthRecord) em formato XLSX
    """
    print("\n" + "="*60)
    print("Exportando registros de saúde dos dispositivos...")
    print("="*60)

    # Calcular datas dinamicamente
    from datetime import datetime, timedelta
    import json
    import urllib.parse

    fim = datetime.now() if end_date is None else _ensure_datetime(end_date, "data final")
    inicio = fim - timedelta(days=2) if start_date is None else _ensure_datetime(start_date, "data inicial")
    if inicio > fim:
        raise ValueError("Data inicial nao pode ser maior que a data final")

    # Formatar datas no padrão MM/DD/YYYY
    data_fim = fim.strftime('%m/%d/%Y')
    data_inicio = inicio.strftime('%m/%d/%Y')

    print(f"Período: {data_inicio} até {data_fim}")

    # Criar o parâmetro timestamp para a URL
    timestamp_str = fim.strftime('%a %b %d %Y %H:%M:%S GMT%z (Brasilia Standard Time)')

    # URL com timestamp
    base_url = "https://portal.visioniot.net/Controllers/SmartDeviceHealthRecord.ashx"
    url = f"{base_url}?v={urllib.parse.quote(timestamp_str)}"

    # Montar o objeto params conforme a requisição original
    params_obj = {
        "action": "export",
        "asArray": 1,
        "filter[0][field]": "EventTime",
        "filter[0][data][type]": "date",
        "filter[0][data][comparison]": "lt",
        "filter[0][data][value]": data_fim,
        "filter[0][data][convert]": False,
        "filter[1][field]": "EventTime",
        "filter[1][data][type]": "date",
        "filter[1][data][comparison]": "gt",
        "filter[1][data][value]": data_inicio,
        "filter[1][data][convert]": False,
        "limit": 50,
        "sort": "SmartDeviceHealthRecordId",
        "dir": "DESC",
        "cols": json.dumps([{"ColumnName":"SmartDeviceHealthRecordId","Header":"Id","Width":100,"Align":"right","Convert":False},{"ColumnName":"EventType","Header":"Event Type","Width":100,"RendererInfo":"","Convert":False},{"ColumnName":"LightIntensity","Header":"Light","Width":50,"Align":"right","RendererInfo":"","Convert":False},{"ColumnName":"LightStatus","Header":"Light Status","Width":160,"Align":"right","Convert":False},{"ColumnName":"Temperature","Header":"Temperature(°C)","Width":110,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":2}","Convert":False},{"ColumnName":"EvaporatorTemperature","Header":"Evaporator Temperature(°C)","Width":110,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":2}","Convert":False},{"ColumnName":"CondensorTemperature","Header":"Condensor Temperature(°C)","Width":110,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":2}","Convert":False},{"ColumnName":"TemperatureInF","Header":"Temperature(°F)","Width":110,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":2}","Convert":False},{"ColumnName":"EvaporatorTemperatureInF","Header":"Evaporator Temperature(°F)","Width":110,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":2}","Convert":False},{"ColumnName":"CondensorTemperatureInF","Header":"Condensor Temperature(°F)","Width":110,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":2}","Convert":False},{"ColumnName":"BatteryLevel","Header":"Battery","Width":70,"Align":"right","RendererInfo":"","Convert":False},{"ColumnName":"BatteryStatus","Header":"Battery Status","Width":160,"Convert":False},{"ColumnName":"HealthInterval","Header":"Interval(Min)","Width":100,"Align":"right","Convert":False},{"ColumnName":"CoolerVoltage","Header":"Cooler Voltage(V)","Width":100,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":2}","Convert":False},{"ColumnName":"MaxVoltage","Header":"Max Voltage(V)","Width":100,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":2}","Convert":False},{"ColumnName":"MinVoltage","Header":"Min Voltage(V)","Width":100,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":2}","Convert":False},{"ColumnName":"AvgPowerConsumption","Header":"Avg Power Consumption(Watt)","Width":125,"Align":"right","Convert":False},{"ColumnName":"TotalCompressorONTime","Header":"Total compressor ON Time(%)","Width":100,"Align":"right","Convert":False},{"ColumnName":"MaxCabinetTemperature","Header":"Max Cabinet Temperature(°C)","Width":100,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":2}","Convert":False},{"ColumnName":"MinCabinetTemperature","Header":"Min Cabinet Temperature(°C)","Width":100,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":2}","Convert":False},{"ColumnName":"AmbientTemperature","Header":"Ambient Temperature(°C)","Width":100,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":2}","Convert":False},{"ColumnName":"MaxCabinetTemperatureInF","Header":"Max Cabinet Temperature(°F)","Width":100,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":2}","Convert":False},{"ColumnName":"MinCabinetTemperatureInF","Header":"Min Cabinet Temperature(°F)","Width":100,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":2}","Convert":False},{"ColumnName":"AmbientTemperatureInF","Header":"Ambient Temperature(°F)","Width":100,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":2}","Convert":False},{"ColumnName":"AppName","Header":"App Name","Width":160,"Align":"right","Convert":False},{"ColumnName":"AppVersion","Header":"App Version","Width":160,"Align":"right","Convert":False},{"ColumnName":"SDKVersion","Header":"SDK Version","Width":160,"Align":"right","Convert":False},{"ColumnName":"UploadedByUser","Header":"Data Uploaded By","Width":160,"Align":"right","Convert":False},{"ColumnName":"AssetCategory","Header":"Asset Category","Width":160,"Align":"right","Convert":False},{"ColumnName":"EventId","Header":"Event Id","Width":70,"Align":"right","Convert":False},{"ColumnName":"CreatedOn","Header":"Created On","Width":180,"RendererInfo":"","Convert":True},{"ColumnName":"EventTime","Header":"Event Time","Width":180,"RendererInfo":"TimeZoneRenderer","Convert":False},{"ColumnName":"GatewayMacAddress","Header":"Gateway Mac","Width":110,"Convert":False},{"ColumnName":"GatewaySerialNumber","Header":"Gateway#","Width":110,"Convert":False},{"ColumnName":"AssetType","Header":"Asset Type","Width":150,"RendererInfo":"","Convert":False},{"ColumnName":"SmartDeviceMonth","Header":"Month","Width":100,"Align":"right","Convert":False},{"ColumnName":"SmartDeviceDay","Header":"Day ","Width":100,"Align":"right","Convert":False},{"ColumnName":"SmartDeviceWeekDay","Header":"Day of Week","Width":100,"Convert":False},{"ColumnName":"SmartDeviceWeek","Header":"Week of Year","Width":100,"Align":"right","Convert":False},{"ColumnName":"AssetSerialNumber","Header":"Asset Serial #","Width":120,"RendererInfo":"","Convert":False},{"ColumnName":"TechnicalIdentificationNumber","Header":"Technical Id","Width":150,"Convert":False},{"ColumnName":"EquipmentNumber","Header":"Equipment Number","Width":150,"Convert":False},{"ColumnName":"MacAddress","Header":"Smart Device Mac","Width":120,"RendererInfo":"","Convert":False},{"ColumnName":"SerialNumber","Header":"Smart Device#","Width":120,"RendererInfo":"","Convert":False},{"ColumnName":"IsSmart","Header":"Is Smart?","Width":80,"Renderer":"Boolean","Convert":False},{"ColumnName":"SmartDeviceTypeName","Header":"Smart Device Type","Width":120,"RendererInfo":"","Convert":False},{"ColumnName":"Location","Header":"Outlet","Width":160,"RendererInfo":"","Convert":False},{"ColumnName":"LocationCode","Header":"Outlet Code","Width":160,"RendererInfo":"","Convert":False},{"ColumnName":"OutletType","Header":"Outlet Type","Width":130,"Convert":False},{"ColumnName":"TimeZone","Header":"Time Zone","Width":300,"RendererInfo":"","Convert":False},{"ColumnName":"ClientName","Header":"Client","Width":100,"Convert":False},{"ColumnName":"SubClientName","Header":"Sub Client","Width":100,"Convert":False}]),
        "exportFileName": "Health Events",
        "exportFormat": "XLSX",
        "filterDescription": f"Event Time Is Less Than {data_fim} AND Event Time Is Greater Than {data_inicio}",
        "selectedFields": json.dumps(["SmartDeviceHealthRecordId as [Id]","EventTypeId as [Event Type]","LightIntensity as [Light]","LightStatus as [Light Status]","Temperature as [Temperature(°C)]","EvaporatorTemperature as [Evaporator Temperature(°C)]","CondensorTemperature as [Condensor Temperature(°C)]","TemperatureInF as [Temperature(°F)]","EvaporatorTemperatureInF as [Evaporator Temperature(°F)]","CondensorTemperatureInF as [Condensor Temperature(°F)]","BatteryLevel as [Battery]","BatteryStatus as [Battery Status]","HealthInterval as [Interval(Min)]","CoolerVoltage as [Cooler Voltage(V)]","MaxVoltage as [Max Voltage(V)]","MinVoltage as [Min Voltage(V)]","AvgPowerConsumption as [Avg Power Consumption(Watt)]","TotalCompressorONTime as [Total compressor ON Time(%)]","MaxCabinetTemperature as [Max Cabinet Temperature(°C)]","MinCabinetTemperature as [Min Cabinet Temperature(°C)]","AmbientTemperature as [Ambient Temperature(°C)]","MaxCabinetTemperatureInF as [Max Cabinet Temperature(°F)]","MinCabinetTemperatureInF as [Min Cabinet Temperature(°F)]","AmbientTemperatureInF as [Ambient Temperature(°F)]","AppName as [App Name]","AppVersion as [App Version]","SDKVersion as [SDK Version]","UploadedByUser as [Data Uploaded By]","AssetCategory as [Asset Category]","EventId as [Event Id]","CreatedOn as [Created On]","EventTime as [Event Time]","GatewayMacAddress as [Gateway Mac]","GatewaySerialNumber as [Gateway#]","AssetTypeId as [Asset Type]","SmartDeviceMonth as [Month]","SmartDeviceDay as [Day ]","SmartDeviceWeekDay as [Day of Week]","SmartDeviceWeek as [Week of Year]","AssetSerialNumber as [Asset Serial #]","TechnicalIdentificationNumber as [Technical Id]","EquipmentNumber as [Equipment Number]","MacAddress as [Smart Device Mac]","SerialNumber as [Smart Device#]","IsSmart as [Is Smart?]","SmartDeviceTypeId as [Smart Device Type]","Location as [Outlet]","LocationCode as [Outlet Code]","OutletType as [Outlet Type]","TimeZoneId as [Time Zone]","ClientName as [Client]","SubClientName as [Sub Client]"]),
        "selectedConcatenatedFields": json.dumps([]),
        "Title": "Health Events",
        "TimeOffSet": -180,
        "dayLightZone": "BRT",
        "normalZone": "BRST",
        "normalOffset": -120,
        "dayLightOffset": -180,
        "timeZone": "America/Sao_Paulo",
        "offSet": 0
    }

    # Payload com o params como string JSON
    payload = {
        'params': json.dumps(params_obj)
    }

    # Headers para requisição de exportação
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Referer': 'https://portal.visioniot.net/default.aspx',
        'Origin': 'https://portal.visioniot.net',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0'
    }

    try:
        response = session.post(url, data=payload, headers=headers)

        print(f"\nStatus: {response.status_code}")

        if response.status_code == 200:
            print("OK - Requisicao bem-sucedida!")

            # Salvar o XLSX (sempre atualiza o mesmo arquivo)
            nome_arquivo = "health_events.xlsx"

            with open(nome_arquivo, 'wb') as f:
                f.write(response.content)

            print(f"\nOK - Arquivo XLSX salvo: {nome_arquivo}")
            print(f"Tamanho: {len(response.content)} bytes")

            # Verificar se o arquivo foi criado
            import os
            if os.path.exists(nome_arquivo):
                print(f"OK - Arquivo verificado e salvo com sucesso!")
            else:
                print(f"ERRO - Falha ao salvar arquivo")

            return nome_arquivo

        else:
            print(f"ERRO - Falha na requisicao: {response.status_code}")
            print(f"Resposta: {response.text[:500]}")
            return None

    except Exception as e:
        print(f"ERRO - Falha ao fazer requisicao: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def buscar_registros_movimento(session, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
    """
    Busca e exporta registros de movimento dos dispositivos (SmartDeviceMovement) em formato XLSX
    """
    print("\n" + "="*60)
    print("Exportando registros de movimento dos dispositivos...")
    print("="*60)

    # Calcular datas dinamicamente
    from datetime import datetime, timedelta
    import json
    import urllib.parse

    fim = datetime.now() if end_date is None else _ensure_datetime(end_date, "data final")
    inicio = fim - timedelta(days=2) if start_date is None else _ensure_datetime(start_date, "data inicial")
    if inicio > fim:
        raise ValueError("Data inicial nao pode ser maior que a data final")

    # Formatar datas no padrão MM/DD/YYYY
    data_fim = fim.strftime('%m/%d/%Y')
    data_inicio = inicio.strftime('%m/%d/%Y')

    print(f"Periodo: {data_inicio} ate {data_fim}")

    # Criar o parâmetro timestamp para a URL
    timestamp_str = fim.strftime('%a %b %d %Y %H:%M:%S GMT%z (Brasilia Standard Time)')

    # URL com timestamp
    base_url = "https://portal.visioniot.net/Controllers/SmartDeviceMovement.ashx"
    url = f"{base_url}?v={urllib.parse.quote(timestamp_str)}"

    # Montar o objeto params conforme a requisição original
    params_obj = {
        "action": "export",
        "asArray": 1,
        "forSmartDeviceMovement": True,
        "filter[0][field]": "EventTime",
        "filter[0][data][type]": "date",
        "filter[0][data][comparison]": "lt",
        "filter[0][data][value]": data_fim,
        "filter[0][data][convert]": False,
        "filter[1][field]": "EventTime",
        "filter[1][data][type]": "date",
        "filter[1][data][comparison]": "gt",
        "filter[1][data][value]": data_inicio,
        "filter[1][data][convert]": False,
        "limit": 50,
        "sort": "SmartDeviceMovementId",
        "dir": "DESC",
        "cols": json.dumps([{"ColumnName":"SmartDeviceMovementId","Header":"Id","Width":100,"Align":"right","Convert":False},{"ColumnName":"MovementType","Header":"Movement Type","Width":120,"Convert":False},{"ColumnName":"StartTime","Header":"Start Time","Width":180,"RendererInfo":"TimeZoneRenderer","Convert":False},{"ColumnName":"EventTime","Header":"End Time","Width":180,"RendererInfo":"TimeZoneRenderer","Convert":False},{"ColumnName":"MovementDuration","Header":"Duration","Width":80,"Align":"right","Convert":False},{"ColumnName":"Latitude","Header":"Latitude","Width":80,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":9}","Convert":False},{"ColumnName":"Longitude","Header":"Longitude","Width":80,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":9}","Convert":False},{"ColumnName":"MovementCount","Header":"Movement Count","Width":100,"Align":"right","Convert":False},{"ColumnName":"IsDoorOpen","Header":"Door Open","Width":70,"Renderer":"Boolean","Convert":False},{"ColumnName":"Displacement","Header":"Displacement(Meter)","Width":100,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":2}","Convert":False},{"ColumnName":"Accuracy","Header":"Accuracy(Meter)","Width":100,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":2}","Convert":False},{"ColumnName":"PowerStatusText","Header":"Power Status","Width":120,"Convert":False},{"ColumnName":"AppName","Header":"App Name","Width":160,"Align":"right","Convert":False},{"ColumnName":"AppVersion","Header":"App Version","Width":160,"Align":"right","Convert":False},{"ColumnName":"SDKVersion","Header":"SDK Version","Width":160,"Align":"right","Convert":False},{"ColumnName":"UploadedByUser","Header":"Data Uploaded By","Width":160,"Align":"right","Convert":False},{"ColumnName":"GPSSource","Header":"GPS Source","Width":120,"Convert":False},{"ColumnName":"EventId","Header":"Event Id","Width":70,"Align":"right","Convert":False},{"ColumnName":"CreatedOn","Header":"Created On","Width":180,"RendererInfo":"","Convert":True},{"ColumnName":"GatewayMacAddress","Header":"Gateway Mac","Width":110,"Convert":False},{"ColumnName":"GatewaySerialNumber","Header":"Gateway#","Width":110,"Convert":False},{"ColumnName":"AssetType","Header":"Asset Type","Width":150,"RendererInfo":"","Convert":False},{"ColumnName":"SmartDeviceMonth","Header":"Month","Width":100,"Align":"right","Convert":False},{"ColumnName":"SmartDeviceDay","Header":"Day ","Width":100,"Align":"right","Convert":False},{"ColumnName":"SmartDeviceWeekDay","Header":"Day of Week","Width":100,"Convert":False},{"ColumnName":"SmartDeviceWeek","Header":"Week of Year","Width":100,"Align":"right","Convert":False},{"ColumnName":"AssetSerialNumber","Header":"Asset Serial #","Width":120,"RendererInfo":"","Convert":False},{"ColumnName":"TechnicalIdentificationNumber","Header":"Technical Id","Width":150,"Convert":False},{"ColumnName":"EquipmentNumber","Header":"Equipment Number","Width":150,"Convert":False},{"ColumnName":"MacAddress","Header":"Smart Device Mac","Width":120,"RendererInfo":"","Convert":False},{"ColumnName":"SerialNumber","Header":"Smart Device#","Width":120,"RendererInfo":"","Convert":False},{"ColumnName":"IsSmart","Header":"Is Smart?","Width":80,"Renderer":"Boolean","Convert":False},{"ColumnName":"SmartDeviceTypeName","Header":"Smart Device Type","Width":120,"RendererInfo":"","Convert":False},{"ColumnName":"Location","Header":"Outlet","Width":160,"RendererInfo":"","Convert":False},{"ColumnName":"LocationCode","Header":"Outlet Code","Width":160,"RendererInfo":"","Convert":False},{"ColumnName":"OutletType","Header":"Outlet Type","Width":130,"Convert":False},{"ColumnName":"TimeZone","Header":"Time Zone","Width":300,"RendererInfo":"","Convert":False},{"ColumnName":"ClientName","Header":"Client","Width":100,"Convert":False},{"ColumnName":"SubClientName","Header":"Sub Client","Width":100,"Convert":False}]),
        "exportFileName": "Movements",
        "exportFormat": "XLSX",
        "filterDescription": f"End Time Is Less Than {data_fim} AND End Time Is Greater Than {data_inicio}",
        "selectedFields": json.dumps(["SmartDeviceMovementId as [Id]","MovementType as [Movement Type]","StartTime as [Start Time]","EventTime as [End Time]","MovementDuration as [Duration]","Latitude as [Latitude]","Longitude as [Longitude]","MovementCount as [Movement Count]","IsDoorOpen as [Door Open]","Displacement as [Displacement(Meter)]","Accuracy as [Accuracy(Meter)]","PowerStatusText as [Power Status]","AppName as [App Name]","AppVersion as [App Version]","SDKVersion as [SDK Version]","UploadedByUser as [Data Uploaded By]","GPSSource as [GPS Source]","EventId as [Event Id]","CreatedOn as [Created On]","GatewayMacAddress as [Gateway Mac]","GatewaySerialNumber as [Gateway#]","AssetTypeId as [Asset Type]","SmartDeviceMonth as [Month]","SmartDeviceDay as [Day ]","SmartDeviceWeekDay as [Day of Week]","SmartDeviceWeek as [Week of Year]","AssetSerialNumber as [Asset Serial #]","TechnicalIdentificationNumber as [Technical Id]","EquipmentNumber as [Equipment Number]","MacAddress as [Smart Device Mac]","SerialNumber as [Smart Device#]","IsSmart as [Is Smart?]","SmartDeviceTypeId as [Smart Device Type]","Location as [Outlet]","LocationCode as [Outlet Code]","OutletType as [Outlet Type]","TimeZoneId as [Time Zone]","ClientName as [Client]","SubClientName as [Sub Client]"]),
        "selectedConcatenatedFields": json.dumps([]),
        "Title": "Movements",
        "TimeOffSet": -180,
        "dayLightZone": "BRT",
        "normalZone": "BRST",
        "normalOffset": -120,
        "dayLightOffset": -180,
        "timeZone": "America/Sao_Paulo",
        "offSet": 0
    }

    # Payload com o params como string JSON
    payload = {
        'params': json.dumps(params_obj)
    }

    # Headers para requisição de exportação
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Referer': 'https://portal.visioniot.net/default.aspx',
        'Origin': 'https://portal.visioniot.net',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0'
    }

    try:
        response = session.post(url, data=payload, headers=headers)

        print(f"\nStatus: {response.status_code}")

        if response.status_code == 200:
            print("OK - Requisicao bem-sucedida!")

            # Salvar o XLSX (sempre atualiza o mesmo arquivo)
            nome_arquivo = "movements.xlsx"

            with open(nome_arquivo, 'wb') as f:
                f.write(response.content)

            print(f"\nOK - Arquivo XLSX salvo: {nome_arquivo}")
            print(f"Tamanho: {len(response.content)} bytes")

            # Verificar se o arquivo foi criado
            import os
            if os.path.exists(nome_arquivo):
                print(f"OK - Arquivo verificado e salvo com sucesso!")
            else:
                print(f"ERRO - Falha ao salvar arquivo")

            return nome_arquivo

        else:
            print(f"ERRO - Falha na requisicao: {response.status_code}")
            print(f"Resposta: {response.text[:500]}")
            return None

    except Exception as e:
        print(f"ERRO - Falha ao fazer requisicao: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def buscar_outlets(session):
    """
    Busca e exporta dados de locais de venda (OutletLocation) em formato XLSX
    """
    print("\n" + "="*60)
    print("Exportando dados de Outlets...")
    print("="*60)

    from datetime import datetime
    import json
    import urllib.parse

    hoje = datetime.now()

    # Criar o parâmetro timestamp para a URL
    timestamp_str = hoje.strftime('%a %b %d %Y %H:%M:%S GMT%z (Brasilia Standard Time)')

    # URL com timestamp
    base_url = "https://portal.visioniot.net/Controllers/OutletLocation.ashx"
    url = f"{base_url}?v={urllib.parse.quote(timestamp_str)}"

    # Montar o objeto params conforme a requisição original - TODAS as colunas do VisionIOT
    params_obj = {
        "action": "export",
        "asArray": 1,
        "start": 0,
        "limit": 50,
        "comboTypes": json.dumps([{"type":"Zone","loaded":False},{"type":"Market","loaded":False},{"type":"PriceType","loaded":False},{"type":"OutletType","loaded":False},{"type":"Country","loaded":False},{"type":"Client","loaded":False}]),
        "sort": "LocationId",
        "dir": "DESC",
        "cols": json.dumps([{"ColumnName":"Name","Header":"Name","Width":200,"Convert":False},{"ColumnName":"Code","Header":"Code","Width":120,"Convert":False},{"ColumnName":"OutletType","Header":"Outlet Type","Width":100,"RendererInfo":"","Convert":False},{"ColumnName":"IsKeyLocation","Header":"Is Key Outlet?","Width":120,"Renderer":"Boolean","Convert":False},{"ColumnName":"IsSmartStatus","Header":"Is Smart?","Width":100,"Renderer":"Boolean","Convert":False},{"ColumnName":"Country","Header":"Country","Width":100,"RendererInfo":"","Convert":False},{"ColumnName":"State","Header":"State","Width":100,"Convert":False},{"ColumnName":"City","Header":"City","Width":100,"Convert":False},{"ColumnName":"Street","Header":"Street","Width":120,"Convert":False},{"ColumnName":"Street2","Header":"Address 2","Width":120,"Convert":False},{"ColumnName":"Street3","Header":"Address 3","Width":120,"Convert":False},{"ColumnName":"Address4","Header":"Address 4","Width":120,"Convert":False},{"ColumnName":"PostalCode","Header":"Postal Code","Width":150,"Convert":False},{"ColumnName":"Retailer","Header":"Retailer","Width":100,"Convert":False},{"ColumnName":"PrimaryPhone","Header":"Primary Phone","Width":120,"Convert":False},{"ColumnName":"PrimarySalesRep","Header":"Primary Sales Rep","Width":120,"Convert":False},{"ColumnName":"LocationSalesRep","Header":"Sales Rep Name","Width":120,"Convert":False},{"ColumnName":"Technician","Header":"Technician","Width":120,"Convert":False},{"ColumnName":"MarketName","Header":"Market","Width":100,"Convert":False},{"ColumnName":"SalesTarget","Header":"Sales Target","Width":100,"Align":"right","Convert":False},{"ColumnName":"Client","Header":"Client","Width":120,"RendererInfo":"","Convert":False},{"ColumnName":"Latitude","Header":"Latitude","Width":60,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":8}","Convert":False},{"ColumnName":"Longitude","Header":"Longitude","Width":70,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":8}","Convert":False},{"ColumnName":"LocationType","Header":"Trade Channel","Width":100,"Convert":False},{"ColumnName":"TradeGroup","Header":"Trade Group","Width":120,"Convert":False},{"ColumnName":"TradeGroupCode","Header":"Trade Group Code","Width":120,"Convert":False},{"ColumnName":"IsActive","Header":"Is Active?","Width":100,"Renderer":"Boolean","Convert":False},{"ColumnName":"Classification","Header":"Customer Tier","Width":120,"Convert":False},{"ColumnName":"SubTradeChannelName","Header":"Sub Trade Channel","Width":120,"Convert":False},{"ColumnName":"SalesOrganizationName","Header":"Sales Organization","Width":150,"Convert":False},{"ColumnName":"SalesOfficeName","Header":"Sales Office","Width":150,"Convert":False},{"ColumnName":"SalesGroupName","Header":"Sales Group","Width":150,"Convert":False},{"ColumnName":"SalesTerritoryName","Header":"Sales Territory","Width":150,"Convert":False},{"ColumnName":"TeleSellingTerritoryName","Header":"TeleSelling Territory Name","Width":150,"Convert":False},{"ColumnName":"BD_TerritoryName","Header":"Business Developer Territory Name","Width":150,"Convert":False},{"ColumnName":"CA_TerritoryName","Header":"Credit Approver Territory Name","Width":150,"Convert":False},{"ColumnName":"MC_TerritoryName","Header":"Merchandizer Territory Name","Width":150,"Convert":False},{"ColumnName":"P1_TerritoryName","Header":"P1_Territory Name","Width":150,"Convert":False},{"ColumnName":"P2_TerritoryName","Header":"P2_Territory Name","Width":150,"Convert":False},{"ColumnName":"P3_TerritoryName","Header":"P3_Territory Name","Width":150,"Convert":False},{"ColumnName":"P4_TerritoryName","Header":"P4_Territory Name","Width":150,"Convert":False},{"ColumnName":"P5_TerritoryName","Header":"P5_Territory Name","Width":150,"Convert":False},{"ColumnName":"NCB_TerritoryName","Header":"Reserve Route Name","Width":150,"Convert":False},{"ColumnName":"RDCustomerName","Header":"RDCustomer Name","Width":120,"Convert":False},{"ColumnName":"TimeZone","Header":"TimeZone","Width":250,"Convert":False},{"ColumnName":"SubClientName","Header":"Sub Client","Width":100,"Convert":False},{"ColumnName":"LocationCluster","Header":"Cluster","Width":120,"Convert":False},{"ColumnName":"MarketSegment","Header":"Market Segment","Width":100,"Convert":False},{"ColumnName":"Segment","Header":"Segment","Width":100,"Convert":False},{"ColumnName":"Environment","Header":"Environment","Width":100,"Convert":False},{"ColumnName":"Assortment1","Header":"Assortment 1","Width":100,"Convert":False},{"ColumnName":"Assortment2","Header":"Assortment 2","Width":100,"Convert":False},{"ColumnName":"Assortment3","Header":"Assortment 3","Width":100,"Convert":False},{"ColumnName":"Assortment4","Header":"Assortment 4","Width":100,"Convert":False},{"ColumnName":"Assortment5","Header":"Assortment 5","Width":100,"Convert":False},{"ColumnName":"Barcode","Header":"BarCode","Width":100,"Convert":False},{"ColumnName":"LocationLocalCluster","Header":"Local Cluster","Width":100,"Convert":False},{"ColumnName":"LocationLocalTradeChannel","Header":"Local TradeChannel","Width":100,"Convert":False},{"ColumnName":"LocationChain","Header":"Chain","Width":100,"Convert":False},{"ColumnName":"RegionName","Header":"Region Name","Width":100,"Convert":False},{"ColumnName":"PrimaryMobile","Header":"Mobile Phone","Width":100,"Convert":False},{"ColumnName":"PrimaryEmail","Header":"Email","Width":100,"Convert":False},{"ColumnName":"CPLName","Header":"CPL Name","Width":100,"Convert":False},{"ColumnName":"ExtraFieldJson","Header":"Extra Field","Width":120,"Convert":False},{"ColumnName":"CreatedOn","Header":"Created On","Width":130,"RendererInfo":"","Convert":True},{"ColumnName":"CreatedByUser","Header":"Created By","Width":150,"Convert":False},{"ColumnName":"ModifiedOn","Header":"Modified On","Width":130,"RendererInfo":"","Convert":True},{"ColumnName":"ModifiedByUser","Header":"Modified By","Width":150,"Convert":False},{"ColumnName":"BDAA","Header":"BDAA","Width":150,"Convert":False},{"ColumnName":"CMMINDCode","Header":"CMMIND","Width":150,"Convert":False},{"ColumnName":"Capacity","Header":"Combined Asset Capacity","Width":180,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":1}","Convert":False},{"ColumnName":"ASMName","Header":"ASM Name","Width":150,"Convert":False},{"ColumnName":"ASMEmail","Header":"ASM Email","Width":150,"Convert":False},{"ColumnName":"TSMName","Header":"TSM Name","Width":150,"Convert":False},{"ColumnName":"TSMEmail","Header":"TSM Email","Width":150,"Convert":False}]),
        "exportFileName": "Outlet",
        "exportFormat": "XLSX",
        "filterDescription": "",
        "selectedFields": json.dumps(["Name as [Name]","Code as [Code]","OutletTypeId as [Outlet Type]","IsKeyLocation as [Is Key Outlet?]","IsSmartStatus as [Is Smart?]","CountryId as [Country]","State as [State]","City as [City]","Street as [Street]","Street2 as [Address 2]","Street3 as [Address 3]","Address4 as [Address 4]","PostalCode as [Postal Code]","Retailer as [Retailer]","PrimaryPhone as [Primary Phone]","PrimarySalesRep as [Primary Sales Rep]","LocationSalesRep as [Sales Rep Name]","Technician as [Technician]","MarketName as [Market]","SalesTarget as [Sales Target]","ClientId as [Client]","Latitude as [Latitude]","Longitude as [Longitude]","LocationType as [Trade Channel]","TradeGroup as [Trade Group]","TradeGroupCode as [Trade Group Code]","IsActive as [Is Active?]","Classification as [Customer Tier]","SubTradeChannelName as [Sub Trade Channel]","SalesOrganizationName as [Sales Organization]","SalesOfficeName as [Sales Office]","SalesGroupName as [Sales Group]","SalesTerritoryName as [Sales Territory]","TeleSellingTerritoryName as [TeleSelling Territory Name]","BD_TerritoryName as [Business Developer Territory Name]","CA_TerritoryName as [Credit Approver Territory Name]","MC_TerritoryName as [Merchandizer Territory Name]","P1_TerritoryName as [P1_Territory Name]","P2_TerritoryName as [P2_Territory Name]","P3_TerritoryName as [P3_Territory Name]","P4_TerritoryName as [P4_Territory Name]","P5_TerritoryName as [P5_Territory Name]","NCB_TerritoryName as [Reserve Route Name]","RDCustomerId as [RDCustomer Name]","TimeZone as [TimeZone]","SubClientName as [Sub Client]","LocationCluster as [Cluster]","MarketSegment as [Market Segment]","Segment as [Segment]","Environment as [Environment]","Assortment1 as [Assortment 1]","Assortment2 as [Assortment 2]","Assortment3 as [Assortment 3]","Assortment4 as [Assortment 4]","Assortment5 as [Assortment 5]","Barcode as [BarCode]","LocationLocalCluster as [Local Cluster]","LocationLocalTradeChannel as [Local TradeChannel]","LocationChain as [Chain]","RegionName as [Region Name]","PrimaryMobile as [Mobile Phone]","PrimaryEmail as [Email]","CPLName as [CPL Name]","ExtraFieldJson as [Extra Field]","CreatedOn as [Created On]","CreatedByUser as [Created By]","ModifiedOn as [Modified On]","ModifiedByUser as [Modified By]","BDAA as [BDAA]","CMMINDCode as [CMMIND]","Capacity as [Combined Asset Capacity]","ASMName as [ASM Name]","ASMEmail as [ASM Email]","TSMName as [TSM Name]","TSMEmail as [TSM Email]"]),
        "selectedConcatenatedFields": json.dumps([]),
        "Title": "Outlet",
        "TimeOffSet": -180,
        "dayLightZone": "BRT",
        "normalZone": "BRST",
        "normalOffset": -120,
        "dayLightOffset": -180,
        "timeZone": "America/Sao_Paulo",
        "offSet": 0
    }

    # Payload com o params como string JSON
    payload = {
        'params': json.dumps(params_obj)
    }

    # Headers para requisição de exportação
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Referer': 'https://portal.visioniot.net/default.aspx',
        'Origin': 'https://portal.visioniot.net',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0'
    }

    try:
        response = session.post(url, data=payload, headers=headers)

        print(f"\nStatus: {response.status_code}")

        if response.status_code == 200:
            print("OK - Requisicao bem-sucedida!")

            # Salvar o XLSX (sempre atualiza o mesmo arquivo)
            nome_arquivo = "outlets.xlsx"

            with open(nome_arquivo, 'wb') as f:
                f.write(response.content)

            print(f"\nOK - Arquivo XLSX salvo: {nome_arquivo}")
            print(f"Tamanho: {len(response.content)} bytes")

            # Verificar se o arquivo foi criado
            import os
            if os.path.exists(nome_arquivo):
                print(f"OK - Arquivo verificado e salvo com sucesso!")
            else:
                print(f"ERRO - Falha ao salvar arquivo")

            return nome_arquivo

        else:
            print(f"ERRO - Falha na requisicao: {response.status_code}")
            print(f"Resposta: {response.text[:500]}")
            return None

    except Exception as e:
        print(f"ERRO - Falha ao fazer requisicao: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def buscar_assets(session):
    """
    Busca e exporta dados de ativos (Asset) em formato XLSX
    """
    print("\n" + "="*60)
    print("Exportando dados de Assets...")
    print("="*60)

    from datetime import datetime
    import json
    import urllib.parse

    hoje = datetime.now()

    # Criar o parâmetro timestamp para a URL
    timestamp_str = hoje.strftime('%a %b %d %Y %H:%M:%S GMT%z (Brasilia Standard Time)')

    # URL com timestamp
    base_url = "https://portal.visioniot.net/Controllers/Asset.ashx"
    url = f"{base_url}?v={urllib.parse.quote(timestamp_str)}"

    # Montar o objeto params conforme a requisição original
    params_obj = {
        "action": "export",
        "asArray": 1,
        "start": 0,
        "limit": 50,
        "comboTypes": json.dumps([{"type":"AssetType","loaded":False},{"type":"State","loaded":False},{"type":"Country","loaded":False},{"type":"AssetCategory","loaded":False},{"type":"SmartDeviceType","loaded":False},{"type":"OutletType","loaded":False},{"type":"Client","loaded":False},{"type":"SubClient","loaded":False},{"type":"TransactionProtocol","loaded":False}]),
        "cols": json.dumps([{"ColumnName":"AssetType","Header":"Asset Type","Width":150,"RendererInfo":"","Convert":False},{"ColumnName":"EquipmentNumber","Header":"Bottler Equipment Number","Width":150,"Convert":False},{"ColumnName":"Location","Header":"Outlet","Width":150,"Convert":False},{"ColumnName":"LocationCode","Header":"Outlet Code","Width":160,"RendererInfo":"","Convert":False},{"ColumnName":"OutletType","Header":"Outlet Type","Width":100,"RendererInfo":"","Convert":False},{"ColumnName":"IsSmartStatus","Header":"Is Smart?","Width":100,"Renderer":"Boolean","Convert":False},{"ColumnName":"IsMissing","Header":"Is Missing?","Width":80,"Renderer":"Boolean","Convert":False},{"ColumnName":"Latitude","Header":"Latitude","Width":80,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":9}","Convert":False},{"ColumnName":"Longitude","Header":"Longitude","Width":80,"Align":"right","Renderer":"Float","RendererInfo":"{\"precision\":9}","Convert":False},{"ColumnName":"ClientName","Header":"Client","Width":120,"RendererInfo":"","Convert":False},{"ColumnName":"City","Header":"City","Width":80,"Convert":False},{"ColumnName":"State","Header":"State","Width":50,"RendererInfo":"","Convert":False},{"ColumnName":"Country","Header":"Country","Width":100,"RendererInfo":"","Convert":False},{"ColumnName":"CreatedOn","Header":"Created On","Width":130,"RendererInfo":"","Convert":True}]),
        "exportFileName": "Assets",
        "exportFormat": "XLSX",
        "filterDescription": "",
        "selectedFields": json.dumps(["AssetTypeId as [Asset Type]","EquipmentNumber as [Bottler Equipment Number]","Location as [Outlet]","LocationCode as [Outlet Code]","OutletTypeId as [Outlet Type]","IsSmart as [Is Smart?]","IsMissing as [Is Missing?]","Latitude as [Latitude]","Longitude as [Longitude]","ClientId as [Client]","City as [City]","StateId as [State]","CountryId as [Country]","CreatedOn as [Created On]"]),
        "selectedConcatenatedFields": json.dumps([]),
        "Title": "Assets",
        "TimeOffSet": -180,
        "dayLightZone": "BRT",
        "normalZone": "BRST",
        "normalOffset": -120,
        "dayLightOffset": -180,
        "timeZone": "America/Sao_Paulo",
        "offSet": 0
    }

    # Payload com o params como string JSON
    payload = {
        'params': json.dumps(params_obj)
    }

    # Headers para requisição de exportação
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Referer': 'https://portal.visioniot.net/default.aspx',
        'Origin': 'https://portal.visioniot.net',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0'
    }

    try:
        response = session.post(url, data=payload, headers=headers)

        print(f"\nStatus: {response.status_code}")

        if response.status_code == 200:
            print("OK - Requisicao bem-sucedida!")

            # Salvar o XLSX (sempre atualiza o mesmo arquivo)
            nome_arquivo = "assets.xlsx"

            with open(nome_arquivo, 'wb') as f:
                f.write(response.content)

            print(f"\nOK - Arquivo XLSX salvo: {nome_arquivo}")
            print(f"Tamanho: {len(response.content)} bytes")

            # Verificar se o arquivo foi criado
            import os
            if os.path.exists(nome_arquivo):
                print(f"OK - Arquivo verificado e salvo com sucesso!")
            else:
                print(f"ERRO - Falha ao salvar arquivo")

            return nome_arquivo

        else:
            print(f"ERRO - Falha na requisicao: {response.status_code}")
            print(f"Resposta: {response.text[:500]}")
            return None

    except Exception as e:
        print(f"ERRO - Falha ao fazer requisicao: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def buscar_smart_devices(session):
    """
    Busca e exporta dados de dispositivos inteligentes (SmartDevice) em formato XLSX
    """
    print("\n" + "="*60)
    print("Exportando dados de Smart Devices...")
    print("="*60)

    from datetime import datetime
    import json
    import urllib.parse

    hoje = datetime.now()

    # Criar o parâmetro timestamp para a URL
    timestamp_str = hoje.strftime('%a %b %d %Y %H:%M:%S GMT%z (Brasilia Standard Time)')

    # URL com timestamp
    base_url = "https://portal.visioniot.net/Controllers/SmartDevice.ashx"
    url = f"{base_url}?v={urllib.parse.quote(timestamp_str)}"

    # Montar o objeto params conforme a requisição original
    params_obj = {
        "action": "export",
        "asArray": 1,
        "CoolerIotClientId": "493",
        "start": 0,
        "limit": 50,
        "comboTypes": json.dumps([{"type":"SmartDeviceTypeCommand","loaded":False},{"type":"SmartDeviceTag","loaded":False},{"type":"AlertRecipientType","loaded":False},{"type":"SmartDeviceStatus","loaded":False},{"type":"Client","loaded":False},{"type":"SmartDeviceTypeCommandUnique","loaded":False},{"type":"TimeZone","loaded":False},{"type":"SmartDeviceType","loaded":False},{"type":"AssetType","loaded":False},{"type":"Country","loaded":False},{"type":"ShippedCountry","loaded":False},{"type":"SubClient","loaded":False},{"type":"UniqueSmartDeviceTypeCommand","loaded":False},{"type":"DisassociationReason","loaded":False}]),
        "cols": json.dumps([{"ColumnName":"SmartDeviceType","Header":"Device Type","Width":130,"RendererInfo":"","Convert":False},{"ColumnName":"ManufacturerName","Header":"Manufacturer","Width":130,"Convert":False},{"ColumnName":"SerialNumber","Header":"Serial Number","Width":150,"Convert":False},{"ColumnName":"MacAddress","Header":"Mac Address","Width":150,"Convert":False},{"ColumnName":"Imei","Header":"IMEI","Width":140,"Convert":False},{"ColumnName":"LastPing","Header":"Last Ping","Width":160,"RendererInfo":"TimeZoneRenderer","Convert":False},{"ColumnName":"Asset","Header":"Linked with Asset","Width":100,"RendererInfo":"","Convert":False},{"ColumnName":"AssociationStatus","Header":"Association","Width":60,"Convert":False},{"ColumnName":"IsMissing","Header":"Is Missing?","Width":80,"Renderer":"Boolean","Convert":False},{"ColumnName":"LocationName","Header":"Outlet","Width":170,"Convert":False},{"ColumnName":"LocationCode","Header":"Outlet Code","Width":100,"RendererInfo":"","Convert":False},{"ColumnName":"OutletType","Header":"Outlet Type","Width":100,"Convert":False},{"ColumnName":"Street","Header":"Street","Width":170,"Convert":False},{"ColumnName":"City","Header":"City","Width":120,"Convert":False},{"ColumnName":"State","Header":"State","Width":100,"Convert":False},{"ColumnName":"Country","Header":"Country","Width":100,"RendererInfo":"","Convert":False},{"ColumnName":"TimeZone","Header":"Time Zone","Width":200,"RendererInfo":"","Convert":False},{"ColumnName":"BatteryLevel","Header":"Battery Level","Width":70,"Align":"right","Convert":False},{"ColumnName":"CreatedOn","Header":"Created On","Width":130,"RendererInfo":"","Convert":True},{"ColumnName":"ModifiedOn","Header":"Modified On","Width":130,"RendererInfo":"","Convert":True}]),
        "exportFileName": "Smart Devices",
        "exportFormat": "XLSX",
        "filterDescription": "",
        "selectedFields": json.dumps(["SmartDeviceTypeId as [Device Type]","ManufacturerName as [Manufacturer]","SerialNumber as [Serial Number]","MacAddress as [Mac Address]","Imei as [IMEI]","LastPing as [Last Ping]","Asset as [Linked with Asset]","AssociationStatus as [Association]","IsMissing as [Is Missing?]","LocationName as [Outlet]","LocationCode as [Outlet Code]","OutletType as [Outlet Type]","Street as [Street]","City as [City]","State as [State]","CountryId as [Country]","TimeZoneId as [Time Zone]","BatteryLevel as [Battery Level]","CreatedOn as [Created On]","ModifiedOn as [Modified On]"]),
        "selectedConcatenatedFields": json.dumps([]),
        "Title": "Smart Devices",
        "TimeOffSet": -180,
        "dayLightZone": "BRT",
        "normalZone": "BRST",
        "normalOffset": -120,
        "dayLightOffset": -180,
        "timeZone": "America/Sao_Paulo",
        "offSet": 0
    }

    # Payload com o params como string JSON
    payload = {
        'params': json.dumps(params_obj)
    }

    # Headers para requisição de exportação
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Referer': 'https://portal.visioniot.net/default.aspx',
        'Origin': 'https://portal.visioniot.net',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0'
    }

    try:
        response = session.post(url, data=payload, headers=headers)

        print(f"\nStatus: {response.status_code}")

        if response.status_code == 200:
            print("OK - Requisicao bem-sucedida!")

            # Salvar o XLSX (sempre atualiza o mesmo arquivo)
            nome_arquivo = "smart_devices.xlsx"

            with open(nome_arquivo, 'wb') as f:
                f.write(response.content)

            print(f"\nOK - Arquivo XLSX salvo: {nome_arquivo}")
            print(f"Tamanho: {len(response.content)} bytes")

            # Verificar se o arquivo foi criado
            import os
            if os.path.exists(nome_arquivo):
                print(f"OK - Arquivo verificado e salvo com sucesso!")
            else:
                print(f"ERRO - Falha ao salvar arquivo")

            return nome_arquivo

        else:
            print(f"ERRO - Falha na requisicao: {response.status_code}")
            print(f"Resposta: {response.text[:500]}")
            return None

    except Exception as e:
        print(f"ERRO - Falha ao fazer requisicao: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def deslogar(session):
    """
    Faz logout do sistema
    """
    print("\n" + "="*60)
    print("Fazendo logout...")
    print("="*60)

    # Verificar se tem cookie de autenticação antes de tentar logout
    has_auth_cookie = any(c.name == '.ASPXAUTH' for c in session.cookies)

    if not has_auth_cookie:
        print("Nenhuma sessao ativa detectada (sem cookie .ASPXAUTH)")
        print("Limpando cookies locais...")
        session.cookies.clear()
        print("OK - Cookies limpos")
        return True

    url = "https://portal.visioniot.net/Controllers/login.ashx"

    payload = {
        'signout': 'true'
    }

    headers = {
        'Accept': '*/*',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'https://portal.visioniot.net/default.aspx',
        'Origin': 'https://portal.visioniot.net',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0'
    }

    try:
        response = session.post(url, data=payload, headers=headers)

        print(f"Status: {response.status_code}")

        if response.status_code == 200:
            # Verificar se a resposta indica sucesso
            import json
            try:
                data = json.loads(response.text)
                # Se retornar "Login failed", significa que não tinha sessão ativa
                if data.get('success') == False and 'Login failed' in data.get('message', ''):
                    print("Aviso - Nenhuma sessao ativa no servidor")
                else:
                    print("OK - Logout realizado com sucesso!")
            except:
                # Se não for JSON, provavelmente retornou HTML/menu (sucesso)
                print("OK - Logout realizado com sucesso!")

            # Sempre limpar cookies localmente
            session.cookies.clear()
            print("OK - Cookies limpos da sessao")

            return True
        else:
            print(f"ERRO - Falha no logout: {response.status_code}")
            # Mesmo com erro, limpar cookies locais
            session.cookies.clear()
            return False

    except Exception as e:
        print(f"ERRO - Falha ao fazer logout: {str(e)}")
        import traceback
        traceback.print_exc()
        # Mesmo com erro, limpar cookies locais
        session.cookies.clear()
        return False

def conectar_banco():
    """
    Conecta ao banco de dados PostgreSQL usando variáveis de ambiente
    """
    try:
        # Pegar a URL do banco de dados do .env
        db_url = os.getenv('URL_DATABASE')

        # Parse da URL: postgresql://user:password@host:port/database
        # Formato: postgresql://postgres:2584@72.60.146.124:5432/portal_associacao_db
        from urllib.parse import urlparse
        result = urlparse(db_url)

        conn = psycopg2.connect(
            host=result.hostname,
            port=result.port,
            user=result.username,
            password=result.password,
            database=result.path[1:]  # Remove a barra inicial
        )

        print("OK - Conectado ao banco de dados")
        return conn
    except Exception as e:
        print(f"ERRO - Falha ao conectar ao banco de dados: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def inserir_health_events(arquivo_xlsx):
    """
    Lê o arquivo XLSX de health events e insere os dados no banco
    """
    print("\n" + "="*60)
    print("Inserindo dados de Health Events no banco...")
    print("="*60)

    try:
        # Ler o arquivo Excel
        df = pd.read_excel(arquivo_xlsx)
        print(f"Total de registros lidos: {len(df)}")

        # Conectar ao banco
        conn = conectar_banco()
        if not conn:
            return False

        cursor = conn.cursor()

        # Preparar query de insert (52 colunas)
        insert_query = """
        INSERT INTO health_events (
            id, event_type, light, light_status, temperature_c, evaporator_temperature_c,
            condensor_temperature_c, temperature_f, evaporator_temperature_f, condensor_temperature_f,
            battery, battery_status, interval_min, cooler_voltage_v, max_voltage_v, min_voltage_v,
            avg_power_consumption_watt, total_compressor_on_time_percent, max_cabinet_temperature_c,
            min_cabinet_temperature_c, ambient_temperature_c, max_cabinet_temperature_f,
            min_cabinet_temperature_f, ambient_temperature_f, app_name, app_version, sdk_version,
            data_uploaded_by, asset_category, event_id, created_on, event_time, gateway_mac,
            gateway_number, asset_type, month, day, day_of_week, week_of_year, asset_serial_number,
            technical_id, equipment_number, smart_device_mac, smart_device_number, is_smart,
            smart_device_type, outlet, outlet_code, outlet_type, time_zone, client, sub_client
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        # Inserir cada linha
        registros_inseridos = 0
        from dateutil import parser as date_parser

        for idx, row in df.iterrows():
            try:
                # Converter valores NaN para None e formatar datas
                valores = []
                for i, val in enumerate(row):
                    if pd.isna(val):
                        valores.append(None)
                    # Índices 30 e 31 são as colunas de data (Created On e Event Time)
                    elif i in [30, 31] and isinstance(val, str):
                        # Converter string de data para timestamp PostgreSQL
                        # Formato original: "27/10/2025 10:27:53 BRST"
                        try:
                            dt = date_parser.parse(val, dayfirst=True)
                            valores.append(dt)
                        except:
                            valores.append(val)
                    else:
                        valores.append(val)

                cursor.execute(insert_query, valores)
                registros_inseridos += 1

                # Mostrar progresso a cada 100 registros
                if registros_inseridos % 100 == 0:
                    print(f"Inseridos {registros_inseridos} registros...")

            except Exception as e:
                print(f"ERRO ao inserir linha {idx}: {str(e)}")
                # Fazer rollback apenas da transação atual
                conn.rollback()
                # Precisamos recriar o cursor após rollback
                cursor = conn.cursor()
                continue

        # Commit das alterações
        conn.commit()
        print(f"\nOK - Total de {registros_inseridos} registros inseridos com sucesso!")

        # Fechar conexão
        cursor.close()
        conn.close()

        return True

    except Exception as e:
        print(f"ERRO ao processar arquivo: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def inserir_movements(arquivo_xlsx):
    """
    Lê o arquivo XLSX de movements e insere os dados no banco
    """
    print("\n" + "="*60)
    print("Inserindo dados de Movements no banco...")
    print("="*60)

    try:
        # Ler o arquivo Excel
        df = pd.read_excel(arquivo_xlsx)
        print(f"Total de registros lidos: {len(df)}")

        # Conectar ao banco
        conn = conectar_banco()
        if not conn:
            return False

        cursor = conn.cursor()

        # Preparar query de insert (39 colunas)
        insert_query = """
        INSERT INTO movements (
            id, movement_type, start_time, end_time, duration, latitude, longitude,
            movement_count, door_open, displacement_meter, accuracy_meter, power_status,
            app_name, app_version, sdk_version, data_uploaded_by, gps_source, event_id,
            created_on, gateway_mac, gateway_number, asset_type, month, day, day_of_week,
            week_of_year, asset_serial_number, technical_id, equipment_number, smart_device_mac,
            smart_device_number, is_smart, smart_device_type, outlet, outlet_code, outlet_type,
            time_zone, client, sub_client
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        # Inserir cada linha
        registros_inseridos = 0
        from dateutil import parser as date_parser

        for idx, row in df.iterrows():
            try:
                # Converter valores NaN para None e formatar datas
                valores = []
                for i, val in enumerate(row):
                    if pd.isna(val):
                        valores.append(None)
                    # Índices 2, 3 e 18 são as colunas de data (Start Time, End Time e Created On)
                    elif i in [2, 3, 18] and isinstance(val, str):
                        # Converter string de data para timestamp PostgreSQL
                        try:
                            dt = date_parser.parse(val, dayfirst=True)
                            valores.append(dt)
                        except:
                            valores.append(val)
                    else:
                        valores.append(val)

                cursor.execute(insert_query, valores)
                registros_inseridos += 1

                # Mostrar progresso a cada 100 registros
                if registros_inseridos % 100 == 0:
                    print(f"Inseridos {registros_inseridos} registros...")

            except Exception as e:
                print(f"ERRO ao inserir linha {idx}: {str(e)}")
                # Fazer rollback apenas da transação atual
                conn.rollback()
                # Precisamos recriar o cursor após rollback
                cursor = conn.cursor()
                continue

        # Commit das alterações
        conn.commit()
        print(f"\nOK - Total de {registros_inseridos} registros inseridos com sucesso!")

        # Fechar conexão
        cursor.close()
        conn.close()

        return True

    except Exception as e:
        print(f"ERRO ao processar arquivo: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def inserir_outlets(arquivo_xlsx):
    """
    Lê o arquivo XLSX de outlets e realiza UPSERT usando Code como chave.
    Colunas mapeadas para o schema atual da tabela outlets.
    """
    print("\n" + "=" * 60)
    print("Inserindo/Atualizando dados de Outlets no banco...")
    print("=" * 60)

    try:
        df = pd.read_excel(arquivo_xlsx)
        print(f"Total de registros lidos: {len(df)}")
        print(f"Colunas: {list(df.columns)}")

        if df.empty:
            print("Planilha vazia. Nada a inserir.")
            return True

        conn = conectar_banco()
        if not conn:
            return False

        cursor = conn.cursor()

        upsert_query = """
        INSERT INTO outlets (
            name, code, outlet_type, is_key_outlet, is_smart,
            country, state, city, street, address_2,
            latitude, longitude, is_active, created_on, modified_on
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (code) DO UPDATE SET
            name = EXCLUDED.name,
            outlet_type = EXCLUDED.outlet_type,
            is_key_outlet = EXCLUDED.is_key_outlet,
            is_smart = EXCLUDED.is_smart,
            country = EXCLUDED.country,
            state = EXCLUDED.state,
            city = EXCLUDED.city,
            street = EXCLUDED.street,
            address_2 = EXCLUDED.address_2,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            is_active = EXCLUDED.is_active,
            modified_on = EXCLUDED.modified_on
        """

        field_map = [
            ("name", "Name", None),
            ("code", "Code", None),
            ("outlet_type", "Outlet Type", None),
            ("is_key_outlet", "Is Key Outlet?", _parse_bool),
            ("is_smart", "Is Smart?", _parse_bool),
            ("country", "Country", None),
            ("state", "State", None),
            ("city", "City", None),
            ("street", "Street", None),
            ("address_2", "Address 2", None),
            ("latitude", "Latitude", _parse_float),
            ("longitude", "Longitude", _parse_float),
            ("is_active", "Is Active?", _parse_bool),
            ("created_on", "Created On", _parse_datetime),
            ("modified_on", "Modified On", _parse_datetime),
        ]

        def get_value(row, excel_col, converter):
            raw = row.get(excel_col) if excel_col in row.index else None
            if converter:
                return converter(raw)
            if raw is None or pd.isna(raw):
                return None
            return raw

        registros_processados = 0

        for idx, row in df.iterrows():
            try:
                dados = {}
                for db_col, excel_col, converter in field_map:
                    dados[db_col] = get_value(row, excel_col, converter)

                if not dados["code"]:
                    print(f"Pulando linha {idx}: Code vazio ou invalido.")
                    continue

                if dados["created_on"] is None:
                    dados["created_on"] = datetime.utcnow()
                if dados["modified_on"] is None:
                    dados["modified_on"] = dados["created_on"]

                valores = [dados[col] for col, _, _ in field_map]
                cursor.execute(upsert_query, tuple(valores))
                registros_processados += 1

                if registros_processados % 100 == 0:
                    print(f"Processados {registros_processados} registros...")

            except Exception as e:
                if registros_processados < 5:
                    print(f"ERRO ao processar linha {idx}: {str(e)}")
                conn.rollback()
                cursor = conn.cursor()
                continue

        conn.commit()
        print(f"\nOK - Total de {registros_processados} registros processados (insert/update) com sucesso!")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"ERRO ao processar arquivo: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def inserir_assets(arquivo_xlsx):
    """
    Lê o arquivo XLSX de assets e faz UPSERT usando Bottler Equipment Number como chave.
    """
    print("\n" + "=" * 60)
    print("Inserindo/Atualizando dados de Assets no banco...")
    print("=" * 60)

    try:
        df = pd.read_excel(arquivo_xlsx)
        print(f"Total de registros lidos: {len(df)}")

        if df.empty:
            print("Planilha vazia. Nada a inserir.")
            return True

        conn = conectar_banco()
        if not conn:
            return False

        cursor = conn.cursor()

        upsert_query = """
        INSERT INTO assets (
            asset_type, bottler_equipment_number, outlet, outlet_code, outlet_type,
            is_missing, latitude, longitude, client, city, state, country,
            created_on, modified_on
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s
        )
        ON CONFLICT (bottler_equipment_number) DO UPDATE SET
            asset_type = EXCLUDED.asset_type,
            outlet = EXCLUDED.outlet,
            outlet_code = EXCLUDED.outlet_code,
            outlet_type = EXCLUDED.outlet_type,
            is_missing = EXCLUDED.is_missing,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            client = EXCLUDED.client,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            country = EXCLUDED.country,
            modified_on = EXCLUDED.modified_on
        """

        field_map = [
            ("asset_type", "Asset Type", None),
            ("bottler_equipment_number", "Bottler Equipment Number", None),
            ("outlet", "Outlet", None),
            ("outlet_code", "Outlet Code", None),
            ("outlet_type", "Outlet Type", None),
            ("is_missing", "Is Missing?", _parse_bool),
            ("latitude", "Latitude", _parse_float),
            ("longitude", "Longitude", _parse_float),
            ("client", "Client", None),
            ("city", "City", None),
            ("state", "State", None),
            ("country", "Country", None),
            ("created_on", "Created On", _parse_datetime),
            ("modified_on", "Created On", _parse_datetime),
        ]

        def get_value(row, excel_col, converter):
            raw = row.get(excel_col) if excel_col in row.index else None
            if converter:
                return converter(raw)
            if raw is None or pd.isna(raw):
                return None
            return raw

        registros_processados = 0

        for idx, row in df.iterrows():
            try:
                dados = {}
                for db_col, excel_col, converter in field_map:
                    dados[db_col] = get_value(row, excel_col, converter)

                if not dados["bottler_equipment_number"]:
                    print(f"Pulando linha {idx}: Bottler Equipment Number vazio ou invalido.")
                    continue

                if dados["created_on"] is None:
                    dados["created_on"] = datetime.utcnow()
                if dados["modified_on"] is None:
                    dados["modified_on"] = dados["created_on"]

                valores = [dados[col] for col, _, _ in field_map]
                cursor.execute(upsert_query, tuple(valores))
                registros_processados += 1

                if registros_processados % 100 == 0:
                    print(f"Processados {registros_processados} registros...")

            except Exception as e:
                if registros_processados < 5:
                    print(f"ERRO ao processar linha {idx}: {str(e)}")
                conn.rollback()
                cursor = conn.cursor()
                continue

        conn.commit()
        print(f"\nOK - Total de {registros_processados} registros processados (insert/update) com sucesso!")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"ERRO ao processar arquivo: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def inserir_smart_devices(arquivo_xlsx):
    """
    Lê o arquivo XLSX de smart devices e faz UPSERT usando MAC Address como chave
    Usa ordem das colunas da planilha, não nomes
    """
    print("\n" + "="*60)
    print("Inserindo/Atualizando dados de Smart Devices no banco...")
    print("="*60)

    try:
        # Ler o arquivo Excel
        df = pd.read_excel(arquivo_xlsx)
        print(f"Total de registros lidos: {len(df)}")

        # Conectar ao banco
        conn = conectar_banco()
        if not conn:
            return False

        cursor = conn.cursor()

        # Preparar query de UPSERT (usando MAC Address como chave)
        upsert_query = """
        INSERT INTO smart_devices (
            device_type, manufacturer, serial_number, mac_address, imei,
            last_ping, linked_asset, association, is_missing, outlet,
            outlet_code, outlet_type, street, city, state, country,
            time_zone, battery_level, created_on, modified_on
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (mac_address) DO UPDATE SET
            device_type = EXCLUDED.device_type,
            manufacturer = EXCLUDED.manufacturer,
            serial_number = EXCLUDED.serial_number,
            imei = EXCLUDED.imei,
            last_ping = EXCLUDED.last_ping,
            linked_asset = EXCLUDED.linked_asset,
            association = EXCLUDED.association,
            is_missing = EXCLUDED.is_missing,
            outlet = EXCLUDED.outlet,
            outlet_code = EXCLUDED.outlet_code,
            outlet_type = EXCLUDED.outlet_type,
            street = EXCLUDED.street,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            country = EXCLUDED.country,
            time_zone = EXCLUDED.time_zone,
            battery_level = EXCLUDED.battery_level,
            modified_on = EXCLUDED.modified_on
        """

        # Inserir/Atualizar cada linha
        registros_processados = 0

        for idx, row in df.iterrows():
            try:
                # Converter valores NaN para None - mantém ordem das colunas
                valores = tuple(None if pd.isna(val) else val for val in row)

                cursor.execute(upsert_query, valores)
                registros_processados += 1

                # Mostrar progresso a cada 100 registros
                if registros_processados % 100 == 0:
                    print(f"Processados {registros_processados} registros...")

            except Exception as e:
                print(f"ERRO ao processar linha {idx}: {str(e)}")
                # Fazer rollback apenas da transação atual
                conn.rollback()
                # Precisamos recriar o cursor após rollback
                cursor = conn.cursor()
                continue

        # Commit das alterações
        conn.commit()
        print(f"\nOK - Total de {registros_processados} registros processados (insert/update) com sucesso!")

        # Fechar conexão
        cursor.close()
        conn.close()

        return True

    except Exception as e:
        print(f"ERRO ao processar arquivo: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    session = logar()
    if not session:
        print("\nERRO - Falha no login. Nao foi possivel buscar dados.")
        return

    print("\nSessao estabelecida. Buscando dados...")

    # Buscar registros de saúde
    dados_saude = buscar_registros_saude(session)

    # Buscar registros de movimento
    dados_movimento = buscar_registros_movimento(session)

    # Buscar dados de outlets
    dados_outlets = buscar_outlets(session)

    # Buscar dados de assets
    dados_assets = buscar_assets(session)

    # Buscar dados de smart devices
    dados_smart_devices = buscar_smart_devices(session)

    # Fazer logout ao terminar
    deslogar(session)

    # Resumo dos arquivos baixados
    print("\n" + "="*60)
    print("RESUMO - DOWNLOAD")
    print("="*60)
    if dados_saude:
        print(f"OK - Arquivo de saude: {dados_saude}")
    else:
        print("ERRO - Falha ao baixar arquivo de saude")

    if dados_movimento:
        print(f"OK - Arquivo de movimento: {dados_movimento}")
    else:
        print("ERRO - Falha ao baixar arquivo de movimento")

    if dados_outlets:
        print(f"OK - Arquivo de outlets: {dados_outlets}")
    else:
        print("ERRO - Falha ao baixar arquivo de outlets")

    if dados_assets:
        print(f"OK - Arquivo de assets: {dados_assets}")
    else:
        print("ERRO - Falha ao baixar arquivo de assets")

    if dados_smart_devices:
        print(f"OK - Arquivo de smart devices: {dados_smart_devices}")
    else:
        print("ERRO - Falha ao baixar arquivo de smart devices")

    # Inserir dados no banco se os downloads foram bem-sucedidos
    if dados_saude and dados_movimento and dados_outlets and dados_assets and dados_smart_devices:
        print("\n" + "="*60)
        print("INICIANDO INSERCAO NO BANCO DE DADOS")
        print("="*60)

        # Inserir health events
        sucesso_health = inserir_health_events(dados_saude)

        # Inserir movements
        sucesso_movements = inserir_movements(dados_movimento)

        # Inserir outlets
        sucesso_outlets = inserir_outlets(dados_outlets)

        # Inserir assets
        sucesso_assets = inserir_assets(dados_assets)

        # Inserir smart devices
        sucesso_smart_devices = inserir_smart_devices(dados_smart_devices)

        # Resumo final
        print("\n" + "="*60)
        print("RESUMO FINAL")
        print("="*60)
        print(f"Download Health Events: {'OK' if dados_saude else 'ERRO'}")
        print(f"Download Movements: {'OK' if dados_movimento else 'ERRO'}")
        print(f"Download Outlets: {'OK' if dados_outlets else 'ERRO'}")
        print(f"Download Assets: {'OK' if dados_assets else 'ERRO'}")
        print(f"Download Smart Devices: {'OK' if dados_smart_devices else 'ERRO'}")
        print(f"Insert Health Events: {'OK' if sucesso_health else 'ERRO'}")
        print(f"Insert Movements: {'OK' if sucesso_movements else 'ERRO'}")
        print(f"Insert Outlets: {'OK' if sucesso_outlets else 'ERRO'}")
        print(f"Insert Assets: {'OK' if sucesso_assets else 'ERRO'}")
        print(f"Insert Smart Devices: {'OK' if sucesso_smart_devices else 'ERRO'}")

        if sucesso_health and sucesso_movements and sucesso_outlets and sucesso_assets and sucesso_smart_devices:
            print("\n✓ Processo concluido com sucesso!")
        else:
            print("\n✗ Processo concluido com erros")
    else:
        print("\n✗ Insercao no banco cancelada devido a falhas no download")


if __name__ == "__main__":
    main()
