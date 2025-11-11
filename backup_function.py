def get_assets_by_client(client_code):
    """
    Get all assets for a specific client
    """
    try:
        db_session = get_session()

        assets = db_session.query(Asset).filter(
            Asset.client == client_code
        ).all()
        assets_data = []
        for asset in assets:
            
            #busca ultimo evento de movement dos ultimos 30 dias
            movement_event = db_session.query(Movement).filter(
            Movement.asset_serial_number == asset.oem_serial_number, Movement.start_time >= (datetime.utcnow() - timedelta(days=30))
            ).order_by(desc(Movement.id)).first()
            
            #se não tiver movement, nem adianta, já que é ele que traz a localização
            if not movement_event:
                continue
            
            #busca o ultimo health event dos ultimos 30 dias
            health_event = db_session.query(HealthEvent).filter(
            HealthEvent.asset_serial_number == asset.oem_serial_number, HealthEvent.event_type == "Cabinet Temperature", HealthEvent.event_time >= (datetime.utcnow() - timedelta(days=30))
            ).order_by(desc(HealthEvent.id)).first()
            
            smart_device = db_session.query(SmartDevice).filter(SmartDevice.serial_number == asset.smart_device).first()

            displacement_alert = db_session.query(Alert).filter(Alert.alert_type == "GPS Displacement", Alert.asset_serial_number == asset.oem_serial_number, Alert.alert_at >= (datetime.utcnow() - timedelta(days=30))).first()

            asset_dict = {
            "bottler_equipment_number": asset.bottler_equipment_number,
            "oem_serial_number": asset.oem_serial_number,
            "outlet": asset.outlet,
            "sub_trade_channel": asset.sub_trade_channel,
            "city": asset.city,
            "state": asset.state,
            "country": asset.country,
            "latest_cabinet_temperature_c": health_event.temperature_c if health_event else None,
            "is_online": smart_device.is_online if smart_device else None,
            "is_missing": True if displacement_alert else False,
            "latest_latitude": movement_event.latitude if movement_event else None,
            "latest_longitude": movement_event.longitude if movement_event else None,
            # 'latest_cabinet_temperature_timestamp': health_event.event_time.isoformat() if health_event else None,
            # "latest_displacement_meter": movement_event.displacement_meter if movement_event else None,
            # 'latest_movement_timestamp': movement_event.start_time.isoformat() if movement_event else None,
            }
            
            #busca os door events dos ultimos 30 dias
            door_event_stats = calculate_door_event_statistics(db_session, asset.oem_serial_number)
            media_manha = door_event_stats["Morning"]["average"]
            media_tarde = door_event_stats["Afternoon"]["average"]
            media_noite = door_event_stats["Night"]["average"]


            # Adiciona as médias ao dicionário do ativo
            asset_dict.update({
                "door_event_average_morning": media_manha,
                "door_event_average_afternoon": media_tarde,
                "door_event_average_night": media_noite
            })
            
            
            city, state, country = check_gps_displacement_alert(db_session, client_code, asset, movement_event)
            if city and state and country:
                asset_dict.update({
                    "city": city,
                    "state": state,
                    "country": country
                })
            assets_data.append(asset_dict)
            print(f"[INFO] Asset data for {asset.oem_serial_number}: {asset_dict}")

        return jsonify({
            "data": assets_data
        })

    except Exception as e:
        print(f"[ERROR] Error fetching assets by client: {str(e)}")