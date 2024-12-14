import os
from discordwebhook import Discord
from datetime import datetime
from utils.logger import setup_logging
from database.database import get_db
from database import models

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
            if not all(key in data for key in ['container_name', 'anomalies_detected', 'total_logs_analyzed', 'called_at', 'detected_at']):
                raise ValueError("Missing required fields in data")

            self.client.post(
                username="MLP Alert",
                avatar_url="https://t3.ftcdn.net/jpg/01/93/90/82/360_F_193908219_ak4aB1PzlhizUVGLOVowzHICc3tl6WeX.jpg",
                embeds=[{
                    "title": "MLP Detection Alert",
                    "description": "컨테이너에서 이상 로그가 감지되었습니다. MLP Detection Alert System에서 알립니다.",
                    "fields": [
                        {
                        "name": "🏠 Container Name",
                        "value": f"[{container_data['host_name']}] {container_data['name']}",
                        "inline": False
                        },
                        {
                        "name": "📦 Container ID",
                        "value": str(data['container_name']),
                        "inline": False
                        },
                        {
                        "name": "⏰ Detected At",
                        "value": str(data['detected_at']),
                        "inline": True
                        },
                        {
                        "name": "⏰ Called At",
                        "value": str(data['called_at']),
                        "inline": True
                        },
                        {
                        "name": "📜 Analyze Report",
                        "value": "-"*20,
                        },
                        {
                        "name": "🔍 Total Logs Analyze",
                        "value": str(data["total_logs_analyzed"]),
                        "inline": True
                        },
                        {
                        "name": "🚨 Anomalies Detected",
                        "value": str(data["anomalies_detected"]),
                        "inline": True
                        }
                    ],
                    "color": 0xFF0000,  
                    "footer": {
                        "text": "MLP Detection Alert System",
                        "icon_url": "https://avatars.githubusercontent.com/u/187281017?v=4"
                    },
                    "timestamp": datetime.now().isoformat()
                }]
            )
            logger.info("Alert sent successfully!")
            
        except Exception as e:
            logger.error(f"Error sending Discord alert: {str(e)}")
            raise