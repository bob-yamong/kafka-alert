import os
from discordwebhook import Discord
from datetime import datetime, timezone

from utils.logger import setup_logging
from database.database import get_db
from database import models

logger = setup_logging(__name__)

alert = None
async def callback(key: str, value: dict) -> None:
    global alert
    try: 
        db = next(get_db())
        try:
            container_query = (
                db.query(models.InternalContainerId, models.Container)
                .join(
                    models.Container,
                    models.Container.id == models.InternalContainerId.container_idx
                )
                .filter(
                    models.InternalContainerId.container_idx == models.Container.id,
                    models.InternalContainerId.mnt_id == value['container_id']['mnt_ns'],
                    models.InternalContainerId.pid_id == value['container_id']['pid_ns']
                )
                .first()
            )
            
            server_name = db.query(models.Server).filter(
                models.Server.id == container_query.Container.host_server
            ).first()

            container_data = None
            if container_query:
                container_data = {
                    "name": container_query.Container.name,
                    "image": container_query.InternalContainerId.image,
                    "server": server_name.name if server_name else "Unknown"
                }
            
            if alert is None:
                webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
                if webhook_url is None:
                    logger.debug("No Discord webhook URL configured")       
                    return
                alert = Alert(webhook_url)
            alert.send_alert(value, container_data)
        except Exception as e:
            import traceback
            logger.error(f"Error sending alert: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            db.close()
    except Exception as e:
        print(f"Error Sending Alert: {str(e)}")
    

class Alert:
    def __init__(self, webhook_url: str):
        self.url = webhook_url
        self.client = Discord(url=webhook_url)
        
    def send_alert(self, data: dict, etc_data: dict = None):
        try:
            # 데이터 검증
            if not all(key in data for key in ['data', 'command', 'process', 'event_id', 'cgroup_id', 'container_id', ]):
                raise ValueError("Missing required fields in data")
            
            ret_val = data["return_value"]

            self.client.post(
                username="Alert",
                avatar_url="https://t3.ftcdn.net/jpg/01/93/90/82/360_F_193908219_ak4aB1PzlhizUVGLOVowzHICc3tl6WeX.jpg",
                embeds=[{
                    "title": "LSM Block Alert",
                    "description": f"[{etc_data["server"]}] 컨테이너에서 {"의심스런" if ret_val == -1 else "아래"} 접근이 {"차단" if ret_val == -1 else "허용"} 되었습니다.",
                    "fields": [
                        {
                        "name": "🚨 Event ID",
                        "value": str(data['event_id']),
                        "inline": True
                        },
                        {
                        "name": "⚡ Command",
                        "value": str(data['command']),
                        "inline": True
                        },
                        {
                        "name": "📁 Path",
                        "value": str(data["data"]['path']),
                        "inline": True
                        },
                        {
                        "name": "🔍 Source",
                        "value": str(data["data"]['source']),
                        "inline": True
                        },
                        {
                        "name": "📦 Container Information",
                        "value": f"NAME: {etc_data["name"] if etc_data else "None"}, Image: {etc_data["image"] if etc_data else "None"}\nMNT_NS: {data['container_id']['mnt_ns']}\nPID_NS: {data['container_id']['pid_ns']}\nCGROUP_ID: {data['cgroup_id']}"
                        },
                        {
                        "name": "👤 Process Information",
                        "value": f"UID: {data['process']['uid']}\nPID: {data['process']['pid']}\nPPID: {data['process']['ppid']}\nHOST_PID: {data['process']['host_pid']}\nHOST_PPID: {data['process']['host_ppid']}"
                        },
                        {
                        "name": "⏰ Timestamp",
                        "value": datetime.strptime(data['timestamp'][:-4], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc).isoformat(),
                        }
                    ],
                    "color": 0xFF0000 if ret_val == -1 else 0x00FF00,  
                    "footer": {
                        "text": "LSM Security Alert System",
                        "icon_url": "https://avatars.githubusercontent.com/u/187281017?v=4"
                    },
                    "timestamp": datetime.now().isoformat()
                }]
            )
            print("Alert sent successfully!")
            
        except Exception as e:
            print(f"Error sending Discord alert: {str(e)}")
            raise