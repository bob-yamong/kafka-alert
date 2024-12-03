import os
from discordwebhook import Discord
from datetime import datetime
from database.database import get_db
from database import models
from utils.logger import setup_logging

logger = setup_logging(__name__)

alert = None
async def callback(key: str, value :dict) -> None:
    global alert
    try: 
        db = next(get_db())
        try:
            container_query = db.query(models.InternalContainerId, models.Container, models.Server)\
                .join(
                    models.Container,
                    models.Container.id == models.InternalContainerId.container_idx
                )\
                .join(
                    models.Server,
                    models.Server.id == models.Container.host_server
                )\
                .filter(models.InternalContainerId.container_id == value['container_name'])\
                .first()    
            
            container_data = None
            if container_query:
                container_data = {
                    "name": container_query.Container.name,
                    "image": container_query.InternalContainerId.image,
                    "host_name": container_query.Server.name
                }
                
            if alert is None:
                webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
                if webhook_url is None:
                    logger.debug("No Discord webhook URL configured")       
                    return
                alert = Alert(webhook_url)
            alert.send_alert(value, container_data)
            
        except Exception as e:
            logger.error(f"Error Sending Alert: {str(e)}")    
        finally:
            db.close()
    except Exception as e:
        print(f"Error Sending Alert: {str(e)}")


class Alert:
    def __init__(self, webhook_url: str):
        self.url = webhook_url
        self.client = Discord(url=webhook_url)
        
    def send_alert(self, data: dict, container_data: dict = None):
        try:
            # 데이터 검증
            if not all(key in data for key in ['container_name', 'pid', 'timestamp', 'detection_time', 'detection_info']):
                raise ValueError("Missing required fields in data")
            
            fields = [
                        {
                        "name": "🏠 Container Name",
                        "value": f"[{container_data['host_name']}] {container_data['name']}",
                        "inline": False
                        },
                        {
                        "name": "📦 Container ID",
                        "value": data['container_name'],
                        "inline": True
                        },
                        {
                        "name": "🚨 PID",
                        "value": data['pid'],
                        "inline": True
                        },
                        {
                        "name": "⏰ Called At",
                        "value": data['timestamp'],
                        },
                        {
                        "name": "⏰ Detected At",
                        "value": data['detection_time'],
                        },
                        {
                        "name": "📜 Detection Info",
                        "value": "-"*20,
                        }
                    ]
            for key, value in data['detection_info'].items():
                fields.append({
                    "name": key,
                    "value": str(value),
                    "inline": True
                })
            self.client.post(
                username="Alert",
                avatar_url="https://t3.ftcdn.net/jpg/01/93/90/82/360_F_193908219_ak4aB1PzlhizUVGLOVowzHICc3tl6WeX.jpg",

                embeds=[{
                    "title": "Suspicious Process Detection",
                    "description": "컨테이너에서 이상 로그가 감지되었습니다. Process Monitoring Security Alert System에서 알립니다.",
                    "fields": fields,
                    "color": 0xFF0000, 
                    "footer": {
                        "text": "Process Monitoring Security Alert System",
                        "icon_url": "https://avatars.githubusercontent.com/u/187281017?v=4"
                    },
                    "timestamp": datetime.now().isoformat()
                }]
            )
            print("Alert sent successfully!")
            
        except Exception as e:
            print(f"Error sending Discord alert: {str(e)}")
            raise