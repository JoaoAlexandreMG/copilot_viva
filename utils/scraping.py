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


def logar(username, password):
    # Criar uma sessão para manter cookies
    session = requests.Session()

    url = "https://portal.visioniot.net/login.aspx?ReturnUrl=%2fdefault.aspx"

    print(f"Passo 1: Fazendo GET inicial para estabelecer sessao para {username}...")
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

    # Payload com credenciais fornecidas
    payload_login = {
        'Username': username,
        'Password': password,
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
            'Username': username,
            'Password': password
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

    # Data final = amanhã, Data inicial = 2 dias atrás
    fim = (datetime.now() + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0) if end_date is None else _ensure_datetime(end_date, "data final")
    inicio = (datetime.now() - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0) if start_date is None else _ensure_datetime(start_date, "data inicial")
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

    # Payload com parâmetros diretos (não encapsulados em 'params')
    payload = params_obj

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
        print(f"Enviando requisição para: {url}")
        print(f"Tamanho do payload: {len(str(payload))} caracteres")
        response = session.post(url, data=payload, headers=headers, timeout=120)

        print(f"\nStatus: {response.status_code}")

        if response.status_code == 200:
            print("OK - Requisicao bem-sucedida!")

            # Salvar o XLSX na pasta docs
            import os
            os.makedirs('docs', exist_ok=True)
            nome_arquivo = "docs/health_events.xlsx"

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

    # Data final = amanhã, Data inicial = 2 dias atrás
    fim = (datetime.now() + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0) if end_date is None else _ensure_datetime(end_date, "data final")
    inicio = (datetime.now() - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0) if start_date is None else _ensure_datetime(start_date, "data inicial")
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

    # Payload com parâmetros diretos (não encapsulados em 'params')
    payload = params_obj

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
        print(f"Enviando requisição para: {url}")
        print(f"Tamanho do payload: {len(str(payload))} caracteres")
        response = session.post(url, data=payload, headers=headers, timeout=120)

        print(f"\nStatus: {response.status_code}")

        if response.status_code == 200:
            print("OK - Requisicao bem-sucedida!")

            # Salvar o XLSX na pasta docs
            import os
            os.makedirs('docs', exist_ok=True)
            nome_arquivo = "docs/movements.xlsx"

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

    # Payload com parâmetros diretos (não encapsulados em 'params')
    payload = params_obj

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
        print(f"Enviando requisição para: {url}")
        print(f"Tamanho do payload: {len(str(payload))} caracteres")
        response = session.post(url, data=payload, headers=headers, timeout=120)

        print(f"\nStatus: {response.status_code}")

        if response.status_code == 200:
            print("OK - Requisicao bem-sucedida!")

            # Salvar o XLSX na pasta docs
            import os
            os.makedirs('docs', exist_ok=True)
            nome_arquivo = "docs/outlets.xlsx"

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

    # Payload com parâmetros diretos (não encapsulados em 'params')
    payload = params_obj

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
        print(f"Enviando requisição para: {url}")
        print(f"Tamanho do payload: {len(str(payload))} caracteres")
        response = session.post(url, data=payload, headers=headers, timeout=120)

        print(f"\nStatus: {response.status_code}")

        if response.status_code == 200:
            print("OK - Requisicao bem-sucedida!")

            # Salvar o XLSX na pasta docs
            import os
            os.makedirs('docs', exist_ok=True)
            nome_arquivo = "docs/assets.xlsx"

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

    # Payload com parâmetros diretos (não encapsulados em 'params')
    payload = params_obj

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
        print(f"Enviando requisição para: {url}")
        print(f"Tamanho do payload: {len(str(payload))} caracteres")
        response = session.post(url, data=payload, headers=headers, timeout=120)

        print(f"\nStatus: {response.status_code}")

        if response.status_code == 200:
            print("OK - Requisicao bem-sucedida!")

            # Salvar o XLSX na pasta docs
            import os
            os.makedirs('docs', exist_ok=True)
            nome_arquivo = "docs/smart_devices.xlsx"

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

def buscar_alerts(session, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
    """
    Busca e exporta dados de alertas (Alert) em formato XLSX
    """
    print("\n" + "="*60)
    print("Exportando dados de Alerts...")
    print("="*60)

    # Calcular datas dinamicamente
    from datetime import datetime, timedelta
    import urllib.parse
    import json

    # Data final = amanhã, Data inicial = 2 dias atrás
    fim = (datetime.now() + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0) if end_date is None else _ensure_datetime(end_date, "data final")
    inicio = (datetime.now() - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0) if start_date is None else _ensure_datetime(start_date, "data inicial")
    if inicio > fim:
        raise ValueError("Data inicial não pode ser maior que a data final")

    # Formatar datas no padrão MM/DD/YYYY
    data_fim = fim.strftime('%m/%d/%Y')
    data_inicio = inicio.strftime('%m/%d/%Y')

    print(f"Período: {data_inicio} até {data_fim}")

    # Criar o parâmetro timestamp para a URL
    timestamp_str = fim.strftime('%a %b %d %Y %H:%M:%S GMT%z (Brasilia Standard Time)')

    # URL com timestamp
    base_url = "https://portal.visioniot.net/Controllers/Alert.ashx"
    url = f"{base_url}?v={urllib.parse.quote(timestamp_str)}"

    # Montar o objeto params conforme a requisição original
    params_obj = {
        "action": "export",
        "asArray": 1,
        "isFromAlert": True,
        "start": 0,
        "limit": 50,
        "filter[0][field]": "AlertAt",
        "filter[0][data][type]": "date",
        "filter[0][data][comparison]": "lt",
        "filter[0][data][value]": data_fim,
        "filter[0][data][convert]": False,
        "filter[1][field]": "AlertAt",
        "filter[1][data][type]": "date",
        "filter[1][data][comparison]": "gt",
        "filter[1][data][value]": data_inicio,
        "filter[1][data][convert]": False,
        "sort": "AlertId",
        "dir": "DESC",
        "cols": json.dumps([{"ColumnName":"AlertId","Header":"Id","Width":100,"Align":"right","Convert":False},{"ColumnName":"AlertType","Header":"Alert Type","Width":100,"RendererInfo":"","Convert":False},{"ColumnName":"AlertText","Header":"Alert Text","Width":200,"Convert":False},{"ColumnName":"AlertDefinition","Header":"Alert Definition","Width":100,"Convert":False},{"ColumnName":"Status","Header":"Status","Width":100,"RendererInfo":"","Convert":False},{"ColumnName":"VisitCheckStatus","Header":"Visit Check","Width":100,"RendererInfo":"","Convert":False},{"ColumnName":"AssetSerialNumber","Header":"Asset Serial#","Width":150,"RendererInfo":"","Convert":False},{"ColumnName":"SmartDeviceSerial","Header":"Smart Device Serial#","Width":150,"Convert":False},{"ColumnName":"AssetEquipmentNumber","Header":"Asset Equipment Number","Width":150,"Convert":False},{"ColumnName":"TechnicalIdentificationNumber","Header":"Asset Technical Identification Number","Width":150,"Convert":False},{"ColumnName":"AssetType","Header":"Asset Type","Width":150,"Convert":False},{"ColumnName":"Street","Header":"Street","Width":150,"Convert":False},{"ColumnName":"Street2","Header":"Street 2","Width":150,"Convert":False},{"ColumnName":"Street3","Header":"Street 3","Width":150,"Convert":False},{"ColumnName":"IsSmart","Header":"Is Smart?","Width":80,"Renderer":"Boolean","Convert":False},{"ColumnName":"AlertAt","Header":"Alert At","Width":150,"RendererInfo":"TimeZoneRenderer","Convert":False},{"ColumnName":"ClosedOn","Header":"Status Changed On","Width":150,"RendererInfo":"TimeZoneRenderer","Convert":False},{"ColumnName":"Priority","Header":"Priority","Width":60,"Convert":False},{"ColumnName":"AlertAgeFormatted","Header":"Age","Width":100,"RendererInfo":"","Convert":False},{"ColumnName":"AlertAgeMins","Header":"Alert Age(in minutes)","Width":130,"Align":"right","Convert":False},{"ColumnName":"AlertValue","Header":"Value","Width":60,"Align":"right","Convert":False},{"ColumnName":"LastUpdatedOn","Header":"Last Update","Width":150,"RendererInfo":"TimeZoneRenderer","Convert":False},{"ColumnName":"Location","Header":"Outlet","Width":160,"RendererInfo":"","Convert":False},{"ColumnName":"LocationCode","Header":"Outlet Code","Width":160,"RendererInfo":"","Convert":False},{"ColumnName":"OutletType","Header":"Outlet Type","Width":130,"Convert":False},{"ColumnName":"City","Header":"Outlet City","Width":160,"Convert":False},{"ColumnName":"ClientName","Header":"Client","Width":100,"Convert":False},{"ColumnName":"TimeZone","Header":"Time Zone","Width":250,"RendererInfo":"","Convert":False},{"ColumnName":"AlertMonth","Header":"Month","Width":100,"Align":"right","Convert":False},{"ColumnName":"AlertDay","Header":"Day ","Width":100,"Align":"right","Convert":False},{"ColumnName":"AlertWeekDay","Header":"Day of Week","Width":100,"Convert":False},{"ColumnName":"AlertWeek","Header":"Week of Year","Width":100,"Align":"right","Convert":False},{"ColumnName":"MarketName","Header":"Market","Width":100,"Convert":False},{"ColumnName":"LocationType","Header":"Trade Channel","Width":100,"Convert":False},{"ColumnName":"Classification","Header":"Customer Tier","Width":120,"Convert":False},{"ColumnName":"SalesOrganizationName","Header":"Sales Organization","Width":150,"Convert":False},{"ColumnName":"SalesOfficeName","Header":"Sales Office","Width":150,"Convert":False},{"ColumnName":"SalesGroupName","Header":"Sales Group","Width":150,"Convert":False},{"ColumnName":"SalesTerritoryName","Header":"Sales Territory","Width":150,"Convert":False},{"ColumnName":"SalesRep","Header":"Sales Rep","Width":120,"Convert":False},{"ColumnName":"IsSystemAlert","Header":"Is System Alert?","Width":100,"Renderer":"Boolean","Convert":False},{"ColumnName":"AcknowledgeComment","Header":"Acknowledge Comment","Width":230,"RendererInfo":"","Convert":False},{"ColumnName":"CreatedOn","Header":"Created On","Width":130,"RendererInfo":"","Convert":True}]),
        "exportFileName": "Alerts",
        "exportFormat": "XLSX",
        "filterDescription": f"Alert At Is Less Than {data_fim} AND Alert At Is Greater Than {data_inicio}",
        "selectedFields": json.dumps(["AlertId as [Id]","AlertTypeId as [Alert Type]","AlertText as [Alert Text]","AlertDefinition as [Alert Definition]","StatusId as [Status]","VisitCheckId as [Visit Check]","AssetSerialNumber as [Asset Serial#]","SmartDeviceSerial as [Smart Device Serial#]","AssetEquipmentNumber as [Asset Equipment Number]","TechnicalIdentificationNumber as [Asset Technical Identification Number]","AssetType as [Asset Type]","Street as [Street]","Street2 as [Street 2]","Street3 as [Street 3]","IsSmart as [Is Smart?]","AlertAt as [Alert At]","ClosedOn as [Status Changed On]","Priority as [Priority]","AlertRaisedOn as [Age]","AlertAgeMins as [Alert Age(in minutes)]","AlertValue as [Value]","LastUpdatedOn as [Last Update]","Location as [Outlet]","LocationCode as [Outlet Code]","OutletType as [Outlet Type]","City as [Outlet City]","ClientName as [Client]","TimeZoneId as [Time Zone]","AlertMonth as [Month]","AlertDay as [Day ]","AlertWeekDay as [Day of Week]","AlertWeek as [Week of Year]","MarketName as [Market]","LocationType as [Trade Channel]","Classification as [Customer Tier]","SalesOrganizationName as [Sales Organization]","SalesOfficeName as [Sales Office]","SalesGroupName as [Sales Group]","SalesTerritoryName as [Sales Territory]","SalesRep as [Sales Rep]","IsSystemAlert as [Is System Alert?]","AcknowledgeComment as [Acknowledge Comment]","CreatedOn as [Created On]"]),
        "selectedConcatenatedFields": json.dumps([]),
        "Title": "Alerts",
        "TimeOffSet": -180,
        "dayLightZone": "BRT",
        "normalZone": "BRST",
        "normalOffset": -120,
        "dayLightOffset": -180,
        "timeZone": "America/Sao_Paulo",
        "offSet": 0
    }

    # Payload com parâmetros diretos (não encapsulados em 'params')
    payload = params_obj

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
        print(f"Enviando requisição para: {url}")
        print(f"Tamanho do payload: {len(str(payload))} caracteres")
        response = session.post(url, data=payload, headers=headers, timeout=120)

        print(f"\nStatus: {response.status_code}")

        if response.status_code == 200:
            print("OK - Requisição bem-sucedida!")

            # Salvar o XLSX na pasta docs
            import os
            os.makedirs('docs', exist_ok=True)
            nome_arquivo = "docs/alerts.xlsx"

            with open(nome_arquivo, 'wb') as f:
                f.write(response.content)

            print(f"\nOK - Arquivo XLSX salvo: {nome_arquivo}")
            print(f"Tamanho: {len(response.content)} bytes")

            # Verificar se o arquivo foi criado
            if os.path.exists(nome_arquivo):
                print(f"OK - Arquivo verificado e salvo com sucesso!")
            else:
                print(f"ERRO - Falha ao salvar arquivo")

            return nome_arquivo

        else:
            print(f"ERRO - Falha na requisição: {response.status_code}")
            print(f"Resposta: {response.text[:500]}")
            return None

    except Exception as e:
        print(f"ERRO - Falha ao fazer requisição: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def buscar_door_statuses(session, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
    """
    Busca e exporta dados de status de porta (SmartDeviceDoorStatus) em formato CSV
    """
    print("\n" + "="*60)
    print("Exportando dados de Door Statuses...")
    print("="*60)

    # Calcular datas dinamicamente
    from datetime import datetime, timedelta
    import urllib.parse
    import json

    # Data final = amanhã, Data inicial = 2 dias atrás
    fim = (datetime.now() + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0) if end_date is None else _ensure_datetime(end_date, "data final")
    inicio = (datetime.now() - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0) if start_date is None else _ensure_datetime(start_date, "data inicial")
    if inicio > fim:
        raise ValueError("Data inicial não pode ser maior que a data final")

    # Formatar datas no padrão MM/DD/YYYY
    data_fim = fim.strftime('%m/%d/%Y')
    data_inicio = inicio.strftime('%m/%d/%Y')

    print(f"Período: {data_inicio} até {data_fim}")

    # Criar o parâmetro timestamp para a URL
    timestamp_str = fim.strftime('%a %b %d %Y %H:%M:%S GMT%z (Brasilia Standard Time)')

    # URL com timestamp
    base_url = "https://portal.visioniot.net/Controllers/SmartDeviceDoorStatus.ashx"
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
        "sort": "SmartDeviceDoorStatusId",
        "dir": "DESC",
        "cols": json.dumps([{"ColumnName":"SmartDeviceDoorStatusId","Header":"Id","Width":100,"Align":"right","Convert":False},{"ColumnName":"DoorOpen","Header":"Open Event Time","Width":180,"RendererInfo":"TimeZoneRenderer","Convert":False},{"ColumnName":"EventTime","Header":"Close Event Time","Width":180,"RendererInfo":"TimeZoneRenderer","Convert":False},{"ColumnName":"EventType","Header":"Event Type","Width":180,"Convert":False},{"ColumnName":"DoorOpenDuration","Header":"Door Open Duration(sec)","Width":90,"Align":"right","Convert":False},{"ColumnName":"SmartDeviceTimeOfDay","Header":"Time of Day","Width":100,"Convert":False},{"ColumnName":"SmartDeviceWeekend","Header":"Weekday / Weekend","Width":100,"Convert":False},{"ColumnName":"HourInDay","Header":"Hour in Day","Width":100,"Align":"right","Convert":False},{"ColumnName":"DoorCount","Header":"Door Count","Width":90,"Align":"right","Convert":False},{"ColumnName":"VisionErrorCodesInfo","Header":"Additional Info","Width":150,"RendererInfo":"","Convert":False},{"ColumnName":"SalesTerritoryName","Header":"Outlet Territory","Width":90,"Convert":False},{"ColumnName":"DoorName","Header":"Door","Width":90,"Convert":False},{"ColumnName":"AssetTypeCapacity","Header":"Capacity Type","Width":150,"Align":"right","Convert":False},{"ColumnName":"DoorOpenTarget","Header":"Door Open Target","Width":150,"Align":"right","Convert":False},{"ColumnName":"DoorOpenTemperature","Header":"Door Open Temperature","Width":150,"Align":"right","Convert":False},{"ColumnName":"DoorCloseTemperature","Header":"Door Close Temperature","Width":150,"Align":"right","Convert":False},{"ColumnName":"AppName","Header":"App Name","Width":160,"Align":"right","Convert":False},{"ColumnName":"AppVersion","Header":"App Version","Width":160,"Align":"right","Convert":False},{"ColumnName":"SDKVersion","Header":"SDK Version","Width":160,"Align":"right","Convert":False},{"ColumnName":"UploadedByUser","Header":"Data Uploaded By","Width":160,"Align":"right","Convert":False},{"ColumnName":"AssetCategory","Header":"Asset Category","Width":160,"Align":"right","Convert":False},{"ColumnName":"EventId","Header":"Event Id","Width":70,"Align":"right","Convert":False},{"ColumnName":"CreatedOn","Header":"Created On","Width":180,"RendererInfo":"","Convert":True},{"ColumnName":"GatewayMacAddress","Header":"Gateway Mac","Width":110,"Convert":False},{"ColumnName":"GatewaySerialNumber","Header":"Gateway#","Width":110,"Convert":False},{"ColumnName":"AssetType","Header":"Asset Type","Width":150,"RendererInfo":"","Convert":False},{"ColumnName":"SmartDeviceMonth","Header":"Month","Width":100,"Align":"right","Convert":False},{"ColumnName":"SmartDeviceDay","Header":"Day ","Width":100,"Align":"right","Convert":False},{"ColumnName":"SmartDeviceWeekDay","Header":"Day of Week","Width":100,"Convert":False},{"ColumnName":"SmartDeviceWeek","Header":"Week of Year","Width":100,"Align":"right","Convert":False},{"ColumnName":"AssetSerialNumber","Header":"Asset Serial #","Width":120,"RendererInfo":"","Convert":False},{"ColumnName":"TechnicalIdentificationNumber","Header":"Technical Id","Width":150,"Convert":False},{"ColumnName":"EquipmentNumber","Header":"Equipment Number","Width":150,"Convert":False},{"ColumnName":"MacAddress","Header":"Smart Device Mac","Width":120,"RendererInfo":"","Convert":False},{"ColumnName":"SerialNumber","Header":"Smart Device#","Width":120,"RendererInfo":"","Convert":False},{"ColumnName":"IsSmart","Header":"Is Smart?","Width":80,"Renderer":"Boolean","Convert":False},{"ColumnName":"SmartDeviceTypeName","Header":"Smart Device Type","Width":120,"RendererInfo":"","Convert":False},{"ColumnName":"Location","Header":"Outlet","Width":160,"RendererInfo":"","Convert":False},{"ColumnName":"LocationCode","Header":"Outlet Code","Width":160,"RendererInfo":"","Convert":False},{"ColumnName":"OutletType","Header":"Outlet Type","Width":130,"Convert":False},{"ColumnName":"TimeZone","Header":"Time Zone","Width":300,"RendererInfo":"","Convert":False},{"ColumnName":"ClientName","Header":"Client","Width":100,"Convert":False},{"ColumnName":"SubClientName","Header":"Sub Client","Width":100,"Convert":False}]),
        "exportFileName": "Door Statuses",
        "exportFormat": "CSV",
        "filterDescription": f"Close Event Time Is Less Than {data_fim} AND Close Event Time Is Greater Than {data_inicio}",
        "selectedFields": json.dumps(["SmartDeviceDoorStatusId as [Id]","DoorOpen as [Open Event Time]","EventTime as [Close Event Time]","EventType as [Event Type]","DoorOpenDuration as [Door Open Duration(sec)]","SmartDeviceTimeOfDay as [Time of Day]","SmartDeviceWeekend as [Weekday / Weekend]","HourInDay as [Hour in Day]","DoorCount as [Door Count]","VisionErrorCodesInfo as [Additional Info]","SalesTerritoryName as [Outlet Territory]","DoorName as [Door]","AssetTypeCapacity as [Capacity Type]","DoorOpenTarget as [Door Open Target]","DoorOpenTemperature as [Door Open Temperature]","DoorCloseTemperature as [Door Close Temperature]","AppName as [App Name]","AppVersion as [App Version]","SDKVersion as [SDK Version]","UploadedByUser as [Data Uploaded By]","AssetCategory as [Asset Category]","EventId as [Event Id]","CreatedOn as [Created On]","GatewayMacAddress as [Gateway Mac]","GatewaySerialNumber as [Gateway#]","AssetTypeId as [Asset Type]","SmartDeviceMonth as [Month]","SmartDeviceDay as [Day ]","SmartDeviceWeekDay as [Day of Week]","SmartDeviceWeek as [Week of Year]","AssetSerialNumber as [Asset Serial #]","TechnicalIdentificationNumber as [Technical Id]","EquipmentNumber as [Equipment Number]","MacAddress as [Smart Device Mac]","SerialNumber as [Smart Device#]","IsSmart as [Is Smart?]","SmartDeviceTypeId as [Smart Device Type]","Location as [Outlet]","LocationCode as [Outlet Code]","OutletType as [Outlet Type]","TimeZoneId as [Time Zone]","ClientName as [Client]","SubClientName as [Sub Client]"]),
        "selectedConcatenatedFields": json.dumps([]),
        "Title": "Door Statuses",
        "TimeOffSet": -180,
        "dayLightZone": "BRT",
        "normalZone": "BRST",
        "normalOffset": -120,
        "dayLightOffset": -180,
        "timeZone": "America/Sao_Paulo",
        "offSet": 0
    }

    # Payload com parâmetros diretos (não encapsulados em 'params')
    payload = params_obj

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
        print(f"Enviando requisição para: {url}")
        print(f"Tamanho do payload: {len(str(payload))} caracteres")
        response = session.post(url, data=payload, headers=headers, timeout=120)

        print(f"\nStatus: {response.status_code}")

        if response.status_code == 200:
            print("OK - Requisição bem-sucedida!")

            # Salvar o CSV na pasta docs
            import os
            os.makedirs('docs', exist_ok=True)
            nome_arquivo = "docs/door_statuses.csv"

            with open(nome_arquivo, 'wb') as f:
                f.write(response.content)

            print(f"\nOK - Arquivo CSV salvo: {nome_arquivo}")
            print(f"Tamanho: {len(response.content)} bytes")

            # Verificar se o arquivo foi criado
            if os.path.exists(nome_arquivo):
                print(f"OK - Arquivo verificado e salvo com sucesso!")
            else:
                print(f"ERRO - Falha ao salvar arquivo")

            return nome_arquivo

        else:
            print(f"ERRO - Falha na requisição: {response.status_code}")
            print(f"Resposta: {response.text[:500]}")
            return None

    except Exception as e:
        print(f"ERRO - Falha ao fazer requisição: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def buscar_users(session):
    """
    Busca e exporta dados de usuários (SecurityAppUser) em formato XLSX
    """
    print("\n" + "="*60)
    print("Exportando dados de Users...")
    print("="*60)

    from datetime import datetime
    import urllib.parse
    import json

    hoje = datetime.now()

    # Criar o parâmetro timestamp para a URL
    timestamp_str = hoje.strftime('%a %b %d %Y %H:%M:%S GMT%z (Brasilia Standard Time)')

    # URL com timestamp
    base_url = "https://portal.visioniot.net/Controllers/SecurityAppUser.ashx"
    url = f"{base_url}?v={urllib.parse.quote(timestamp_str)}"

    # Montar o objeto params conforme a requisição original
    params_obj = {
        "action": "export",
        "asArray": 1,
        "start": 0,
        "limit": 50,
        "comboTypes": json.dumps([]),
        "sort": "UserId",
        "dir": "DESC",
        "cols": json.dumps([{"ColumnName":"FirstName","Header":"First Name","Width":100,"Convert":False},{"ColumnName":"LastName","Header":"Last Name","Width":100,"Convert":False},{"ColumnName":"Username","Header":"User Name","Width":100,"Convert":False},{"ColumnName":"UPN","Header":"UPN","Width":100,"Convert":False},{"ColumnName":"PrimaryEmail","Header":"Email","Width":150,"Convert":False},{"ColumnName":"PrimaryPhone","Header":"Phone","Width":100,"Convert":False},{"ColumnName":"Role","Header":"Role","Width":100,"Convert":False},{"ColumnName":"ReportingManager","Header":"Reporting Manager","Width":100,"Convert":False},{"ColumnName":"PreferedNotificationType","Header":"Preferred Notification Type","Width":100,"Convert":False},{"ColumnName":"Country","Header":"Country","Width":150,"Convert":False},{"ColumnName":"ResponsibleCountry","Header":"Responsible Country","Width":100,"Convert":False},{"ColumnName":"IsActive","Header":"Is Active?","Width":100,"Renderer":"Boolean","Convert":False},{"ColumnName":"SalesOrganization","Header":"Sales Organization","Width":100,"Convert":False},{"ColumnName":"SalesOffice","Header":"Sales Office","Width":100,"Convert":False},{"ColumnName":"SalesGroup","Header":"Sales Group","Width":100,"Convert":False},{"ColumnName":"SalesTerritory","Header":"Sales Territory","Width":100,"Convert":False},{"ColumnName":"TeleSellingTerritoryName","Header":"Teleselling Territory","Width":100,"Convert":False},{"ColumnName":"BD_Territory","Header":"BD Territory Name","Width":150,"Convert":False},{"ColumnName":"CA_Territory","Header":"CA Territory Name","Width":150,"Convert":False},{"ColumnName":"MC_Territory","Header":"MC Territory Name","Width":150,"Convert":False},{"ColumnName":"P1_Territory","Header":"P1 Territory Name","Width":150,"Convert":False},{"ColumnName":"P2_Territory","Header":"P2 Territory Name","Width":150,"Convert":False},{"ColumnName":"P3_Territory","Header":"P3 Territory Name","Width":150,"Convert":False},{"ColumnName":"P4_Territory","Header":"P4 Territory Name","Width":150,"Convert":False},{"ColumnName":"P5_Territory","Header":"P5 Territory Name","Width":150,"Convert":False},{"ColumnName":"NCB_Territory","Header":"NCB Territory Name","Width":150,"Convert":False},{"ColumnName":"LastLoginOn","Header":"Last Login On","Width":150,"RendererInfo":"","Convert":True},{"ColumnName":"ClientName","Header":"Client","Width":100,"Convert":False},{"ColumnName":"CreatedOn","Header":"Created On","Width":130,"RendererInfo":"","Convert":True},{"ColumnName":"CreatedByUser","Header":"Created By","Width":150,"Convert":False},{"ColumnName":"ModifiedOn","Header":"Modified On","Width":130,"RendererInfo":"","Convert":True},{"ColumnName":"ModifiedByUser","Header":"Modified By","Width":150,"Convert":False},{"ColumnName":"RewardPoint","Header":"Reward Point","Width":150,"Convert":False}]),
        "exportFileName": "Users",
        "exportFormat": "XLSX",
        "filterDescription": "",
        "selectedFields": json.dumps(["FirstName as [First Name]","LastName as [Last Name]","Username as [User Name]","UPN as [UPN]","PrimaryEmail as [Email]","PrimaryPhone as [Phone]","Role as [Role]","ReportingManager as [Reporting Manager]","PreferedNotificationType as [Preferred Notification Type]","Country as [Country]","ResponsibleCountry as [Responsible Country]","IsActive as [Is Active?]","SalesOrganization as [Sales Organization]","SalesOffice as [Sales Office]","SalesGroup as [Sales Group]","SalesTerritory as [Sales Territory]","TeleSellingTerritoryName as [Teleselling Territory]","BD_Territory as [BD Territory Name]","CA_Territory as [CA Territory Name]","MC_Territory as [MC Territory Name]","P1_Territory as [P1 Territory Name]","P2_Territory as [P2 Territory Name]","P3_Territory as [P3 Territory Name]","P4_Territory as [P4 Territory Name]","P5_Territory as [P5 Territory Name]","NCB_Territory as [NCB Territory Name]","LastLoginOn as [Last Login On]","ClientName as [Client]","CreatedOn as [Created On]","CreatedByUser as [Created By]","ModifiedOn as [Modified On]","ModifiedByUser as [Modified By]","RewardPoint as [Reward Point]"]),
        "selectedConcatenatedFields": json.dumps([]),
        "Title": "Users",
        "TimeOffSet": -180,
        "dayLightZone": "BRT",
        "normalZone": "BRST",
        "normalOffset": -120,
        "dayLightOffset": -180,
        "timeZone": "America/Sao_Paulo",
        "offSet": 0
    }

    # Payload com parâmetros diretos (não encapsulados em 'params')
    payload = params_obj

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
        print(f"Enviando requisição para: {url}")
        print(f"Tamanho do payload: {len(str(payload))} caracteres")
        response = session.post(url, data=payload, headers=headers, timeout=120)

        print(f"\nStatus: {response.status_code}")

        if response.status_code == 200:
            print("OK - Requisição bem-sucedida!")

            # Salvar o XLSX na pasta docs
            import os
            os.makedirs('docs', exist_ok=True)
            nome_arquivo = "docs/users.xlsx"

            with open(nome_arquivo, 'wb') as f:
                f.write(response.content)

            print(f"\nOK - Arquivo XLSX salvo: {nome_arquivo}")
            print(f"Tamanho: {len(response.content)} bytes")

            # Verificar se o arquivo foi criado
            if os.path.exists(nome_arquivo):
                print(f"OK - Arquivo verificado e salvo com sucesso!")
            else:
                print(f"ERRO - Falha ao salvar arquivo")

            return nome_arquivo

        else:
            print(f"ERRO - Falha na requisição: {response.status_code}")
            print(f"Resposta: {response.text[:500]}")
            return None

    except Exception as e:
        print(f"ERRO - Falha ao fazer requisição: {str(e)}")
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
        import sys
        import os
        
        # Adicionar o diretório pai ao path se necessário
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)
        
        # Carregar .env se necessário
        from dotenv import load_dotenv
        load_dotenv()
        
        # Pegar a URL do banco de dados do .env
        db_url = os.getenv('DATABASE_URL')
        
        if not db_url:
            raise ValueError("DATABASE_URL não encontrada no .env")

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


# ============================================================================
# FUNÇÕES DE INTEGRAÇÃO COM BANCO DE DADOS
# ============================================================================

def buscar_contas_vision():
    """
    Busca todas as contas VisionAccount da base de dados
    """
    try:
        import sys
        import os
        
        # Adicionar o diretório pai ao path se necessário
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)
        
        from db.database import get_session
        from models.models import VisionAccount, Client
        
        session = get_session()
        
        # Buscar contas e clientes separadamente, depois fazer o match manual
        contas = session.query(VisionAccount).all()
        
        if not contas:
            print("Nenhuma conta VisionAccount encontrada.")
            return []
        
        contas_formatadas = []
        for conta in contas:
            # Tentar encontrar o cliente pelo client_id
            client = session.query(Client).filter(Client.id == conta.client_id).first()
            if not client:
                # Tentar encontrar pelo client_code se não encontrar por id
                client = session.query(Client).filter(Client.client_code == conta.client_id).first()
            
            client_name = client.client_name if client else f"Client_{conta.client_id}"
            contas_formatadas.append((conta, client_name))
        
        session.close()
        return contas_formatadas
        
    except Exception as e:
        print(f"ERRO ao buscar contas: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

def processar_cliente_dados_estaticos(username, password, client_name):
    """
    Processa um cliente específico para baixar dados estáticos: assets, outlets e smartdevices
    Estes dados não mudam frequentemente e devem ser baixados periodicamente
    """
    print(f"\n{'='*80}")
    print(f"PROCESSANDO CLIENTE (DADOS ESTÁTICOS): {client_name} (Username: {username})")
    print(f"{'='*80}")
    
    # Fazer login
    session = logar(username, password)
    if not session:
        print(f"ERRO - Falha no login para {client_name}. Pulando cliente.")
        return False
    
    print(f"Login realizado com sucesso para {client_name}")
    
    # Prefixar arquivos com o client_name para evitar sobreposição
    arquivos_baixados = {}
    
    try:
        # Buscar dados de outlets
        print(f"\nBuscando outlets para {client_name}...")
        arquivo_outlets = buscar_outlets(session)
        if arquivo_outlets:
            import os
            nome_novo = f"docs/{client_name.replace(' ', '_')}_outlets.xlsx"
            os.rename(arquivo_outlets, nome_novo)
            arquivos_baixados['outlets'] = nome_novo
            print(f"OK - Arquivo de outlets salvo: {nome_novo}")
        
        # Buscar dados de assets
        print(f"\nBuscando assets para {client_name}...")
        arquivo_assets = buscar_assets(session)
        if arquivo_assets:
            import os
            nome_novo = f"docs/{client_name.replace(' ', '_')}_assets.xlsx"
            os.rename(arquivo_assets, nome_novo)
            arquivos_baixados['assets'] = nome_novo
            print(f"OK - Arquivo de assets salvo: {nome_novo}")
        
        # Buscar dados de smart devices
        print(f"\nBuscando smart devices para {client_name}...")
        arquivo_smart_devices = buscar_smart_devices(session)
        if arquivo_smart_devices:
            import os
            nome_novo = f"docs/{client_name.replace(' ', '_')}_smart_devices.xlsx"
            os.rename(arquivo_smart_devices, nome_novo)
            arquivos_baixados['smart_devices'] = nome_novo
            print(f"OK - Arquivo de smart devices salvo: {nome_novo}")
        
    except Exception as e:
        print(f"ERRO durante o processamento de dados estáticos para {client_name}: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Sempre fazer logout
        print(f"\nFazendo logout de {client_name}...")
        deslogar(session)
    
    # Verificar se todos os downloads foram bem-sucedidos
    if len(arquivos_baixados) == 3:  # Todos os 3 tipos de dados estáticos
        print(f"\n{'='*60}")
        print(f"DOWNLOAD ESTÁTICO COMPLETO - {client_name}")
        print(f"{'='*60}")
        
        # Mostrar arquivos baixados
        for tipo, arquivo in arquivos_baixados.items():
            print(f"{tipo.replace('_', ' ').title()}: {arquivo}")
        
        print(f"\nTodos os dados estáticos de {client_name} foram baixados com sucesso!")
        return True
    else:
        print(f"\nERRO - Download de dados estáticos incompleto para {client_name}.")
        print(f"Arquivos baixados ({len(arquivos_baixados)}/3):")
        for tipo, arquivo in arquivos_baixados.items():
            print(f"  - {tipo}: {arquivo}")
        return False

def processar_cliente_dados_diarios(username, password, client_name, start_date=None, end_date=None):
    """
    Processa um cliente específico para baixar dados diários: health events, movements, alerts, door statuses e users
    Esta função deve ser executada diariamente
    """
    print(f"\n{'='*80}")
    print(f"PROCESSANDO CLIENTE (DADOS DIÁRIOS): {client_name} (Username: {username})")
    print(f"{'='*80}")
    
    # Fazer login
    session = logar(username, password)
    if not session:
        print(f"ERRO - Falha no login para {client_name}. Pulando cliente.")
        return False
    
    print(f"Login realizado com sucesso para {client_name}")
    
    # Prefixar arquivos com o client_name para evitar sobreposição
    arquivos_baixados = {}
    
    try:
        # Buscar registros de saúde
        print(f"\nBuscando registros de saúde para {client_name}...")
        arquivo_saude = buscar_registros_saude(session, start_date, end_date)
        if arquivo_saude:
            import os
            nome_novo = f"docs/{client_name.replace(' ', '_')}_health_events.xlsx"
            os.rename(arquivo_saude, nome_novo)
            arquivos_baixados['health_events'] = nome_novo
            print(f"OK - Arquivo de saúde salvo: {nome_novo}")
        
        # Buscar registros de movimento
        print(f"\nBuscando registros de movimento para {client_name}...")
        arquivo_movimento = buscar_registros_movimento(session, start_date, end_date)
        if arquivo_movimento:
            import os
            nome_novo = f"docs/{client_name.replace(' ', '_')}_movements.xlsx"
            os.rename(arquivo_movimento, nome_novo)
            arquivos_baixados['movements'] = nome_novo
            print(f"OK - Arquivo de movimento salvo: {nome_novo}")
        
        # Buscar dados de alerts
        print(f"\nBuscando alerts para {client_name}...")
        arquivo_alerts = buscar_alerts(session, start_date, end_date)
        if arquivo_alerts:
            import os
            nome_novo = f"docs/{client_name.replace(' ', '_')}_alerts.xlsx"
            os.rename(arquivo_alerts, nome_novo)
            arquivos_baixados['alerts'] = nome_novo
            print(f"OK - Arquivo de alerts salvo: {nome_novo}")
        
        # Buscar dados de door statuses
        print(f"\nBuscando door statuses para {client_name}...")
        arquivo_door_statuses = buscar_door_statuses(session, start_date, end_date)
        if arquivo_door_statuses:
            import os
            nome_novo = f"docs/{client_name.replace(' ', '_')}_door_statuses.csv"
            os.rename(arquivo_door_statuses, nome_novo)
            arquivos_baixados['door_statuses'] = nome_novo
            print(f"OK - Arquivo de door statuses salvo: {nome_novo}")
        
        # Buscar dados de users
        print(f"\nBuscando users para {client_name}...")
        arquivo_users = buscar_users(session)
        if arquivo_users:
            import os
            nome_novo = f"docs/{client_name.replace(' ', '_')}_users.xlsx"
            os.rename(arquivo_users, nome_novo)
            arquivos_baixados['users'] = nome_novo
            print(f"OK - Arquivo de users salvo: {nome_novo}")
        
    except Exception as e:
        print(f"ERRO durante o processamento de dados diários para {client_name}: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Sempre fazer logout
        print(f"\nFazendo logout de {client_name}...")
        deslogar(session)
    
    # Verificar se todos os downloads foram bem-sucedidos
    if len(arquivos_baixados) == 5:  # Todos os 5 tipos de dados diários
        print(f"\n{'='*60}")
        print(f"DOWNLOAD DIÁRIO COMPLETO - {client_name}")
        print(f"{'='*60}")
        
        # Mostrar arquivos baixados
        for tipo, arquivo in arquivos_baixados.items():
            print(f"{tipo.replace('_', ' ').title()}: {arquivo}")
        
        print(f"\nTodos os dados diários de {client_name} foram baixados com sucesso!")
        print(f"Para inserir no banco de dados, execute: python3 import_all_data.py")
        
        return True
    else:
        print(f"\nERRO - Download de dados diários incompleto para {client_name}.")
        print(f"Arquivos baixados ({len(arquivos_baixados)}/5):")
        for tipo, arquivo in arquivos_baixados.items():
            print(f"  - {tipo}: {arquivo}")
        return False

def main():
    """
    Função principal que processa todos os clientes
    """
    print("INICIANDO SCRAPING MULTI-CLIENTE")
    print("="*80)
    
    # Buscar todas as contas da base de dados
    contas = buscar_contas_vision()
    
    if not contas:
        print("ERRO - Nenhuma conta VisionAccount encontrada na base de dados")
        return
    
    print(f"Encontradas {len(contas)} contas para processar:")
    for conta_data in contas:
        conta, client_name = conta_data
        print(f"  - {client_name} (Username: {conta.username})")
    
    # Processar cada cliente - DADOS DIÁRIOS (executar diariamente)
    resultados = {}
    
    for conta_data in contas:
        conta, client_name = conta_data
        
        try:
            sucesso = processar_cliente_dados_diarios(
                username=conta.username,
                password=conta.password,
                client_name=client_name
            )
            resultados[client_name] = sucesso
        except Exception as e:
            print(f"ERRO CRÍTICO ao processar {client_name}: {str(e)}")
            import traceback
            traceback.print_exc()
            resultados[client_name] = False
    
    # Resumo final
    print(f"\n{'='*80}")
    print("RESUMO FINAL - TODOS OS CLIENTES")
    print(f"{'='*80}")
    
    for client_name, sucesso in resultados.items():
        status = "✓ SUCESSO" if sucesso else "✗ ERRO"
        print(f"{client_name}: {status}")
    
    sucessos = sum(1 for s in resultados.values() if s)
    total = len(resultados)
    
    print(f"\nTotal processado: {sucessos}/{total} clientes com sucesso")
    
    if sucessos == total:
        print("✓ Todos os clientes processados com sucesso!")
        
        # Executar automaticamente o script de importação
        print(f"\n{'='*60}")
        print("INICIANDO IMPORTAÇÃO AUTOMÁTICA DOS DADOS")
        print(f"{'='*60}")
        
        try:
            import subprocess
            import sys
            
            # Executar o script import_all_data.py
            result = subprocess.run([
                sys.executable, 
                "import_all_data.py"
            ], 
            capture_output=True, 
            text=True,
            cwd="."  # Executar no diretório atual
            )
            
            if result.returncode == 0:
                print("✓ Importação automática concluída com sucesso!")
                if result.stdout:
                    print("\nSaída da importação:")
                    print(result.stdout)
            else:
                print("✗ Erro durante a importação automática:")
                if result.stderr:
                    print("Erro:", result.stderr)
                if result.stdout:
                    print("Saída:", result.stdout)
                    
        except Exception as e:
            print(f"✗ Erro ao executar importação automática: {str(e)}")
            print("Execute manualmente: python3 import_all_data.py")
    else:
        print("✗ Alguns clientes falharam durante o processamento")
        print("Importação automática cancelada devido a erros no download")
        print("Verifique os erros e execute manualmente: python3 import_all_data.py")

def main_dados_estaticos():
    """
    Função principal para baixar dados estáticos (assets, outlets, smartdevices)
    Execute esta função periodicamente (ex: uma vez por semana ou mês)
    """
    print("INICIANDO SCRAPING DE DADOS ESTÁTICOS (ASSETS, OUTLETS, SMARTDEVICES)")
    print("="*80)
    
    # Buscar todas as contas da base de dados
    contas = buscar_contas_vision()
    
    if not contas:
        print("ERRO - Nenhuma conta VisionAccount encontrada na base de dados")
        return
    
    print(f"Encontradas {len(contas)} contas para processar:")
    for conta_data in contas:
        conta, client_name = conta_data
        print(f"  - {client_name} (Username: {conta.username})")
    
    # Processar cada cliente - DADOS ESTÁTICOS (executar periodicamente)
    resultados = {}
    
    for conta_data in contas:
        conta, client_name = conta_data
        
        try:
            sucesso = processar_cliente_dados_estaticos(
                username=conta.username,
                password=conta.password,
                client_name=client_name
            )
            resultados[client_name] = sucesso
        except Exception as e:
            print(f"ERRO CRÍTICO ao processar {client_name}: {str(e)}")
            import traceback
            traceback.print_exc()
            resultados[client_name] = False
    
    # Resumo final
    print(f"\n{'='*80}")
    print("RESUMO FINAL - DADOS ESTÁTICOS")
    print(f"{'='*80}")
    
    for client_name, sucesso in resultados.items():
        status = "✓ SUCESSO" if sucesso else "✗ ERRO"
        print(f"{client_name}: {status}")
    
    sucessos = sum(1 for s in resultados.values() if s)
    total = len(resultados)
    
    print(f"\nTotal processado: {sucessos}/{total} clientes com sucesso")
    
    if sucessos == total:
        print("✓ Todos os clientes processados com sucesso!")
        
        # Executar automaticamente o script de importação
        print(f"\n{'='*60}")
        print("INICIANDO IMPORTAÇÃO AUTOMÁTICA DOS DADOS ESTÁTICOS")
        print(f"{'='*60}")
        
        try:
            import subprocess
            import sys
            
            # Executar o script import_all_data.py
            result = subprocess.run([
                sys.executable, 
                "import_all_data.py"
            ], 
            capture_output=True, 
            text=True,
            cwd="."  # Executar no diretório atual
            )
            
            if result.returncode == 0:
                print("✓ Importação automática concluída com sucesso!")
                if result.stdout:
                    print("\nSaída da importação:")
                    print(result.stdout)
            else:
                print("✗ Erro durante a importação automática:")
                if result.stderr:
                    print("Erro:", result.stderr)
                if result.stdout:
                    print("Saída:", result.stdout)
                    
        except Exception as e:
            print(f"✗ Erro ao executar importação automática: {str(e)}")
            print("Execute manualmente: python3 import_all_data.py")
    else:
        print("✗ Alguns clientes falharam durante o processamento")
        print("Importação automática cancelada devido a erros no download")
        print("Verifique os erros e execute manualmente: python3 import_all_data.py")



if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "static":
        # Executar scraping de dados estáticos
        main_dados_estaticos()
    else:
        # Executar scraping de dados diários (padrão)
        main()
