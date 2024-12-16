from sqlalchemy.orm import Session
from datetime import datetime

from database.database import get_db
from database import models

async def callback(key: str, value: dict) -> bool:
    db = next(get_db())
    try: 
        add_heartbeat(db, value)
    except Exception as e:
        print(f"Error Saving Heartbeat: {str(e)}")
        raise
    return True

def add_heartbeat(db: Session, heartbeat: dict):
    with db.begin():
        server = _get_or_create_server(db, heartbeat)
        _add_system_info(db, server.id, heartbeat["host"], heartbeat["timestamp"])
        _handle_containers(db, server.id, heartbeat["containers"])
        
        db.add(models.Heartbeat(
            uuid=heartbeat["host_uuid"],
            survival_container_cnt=len(heartbeat["containers"]),
            req_ip="unused_field",
            endpoint="unused_field"
        ))
        
    db.commit()

def _get_or_create_server(db: Session, heartbeat: dict):
    server = db.query(models.Server).filter(models.Server.uuid == heartbeat["host_uuid"]).first()
    
    if server and server.name != heartbeat["server_name"]:
        db.query(models.Server).filter(models.Server.id == server.id).update({"name": heartbeat["server_name"]})
        db.commit()
        server.uuid = heartbeat["host_uuid"]
    
    if not server:
        db.add(models.Server(
            uuid=heartbeat["host_uuid"],
            name=heartbeat["server_name"]
        ))
        db.commit()
        
    return server


def _add_system_info(db: Session, server_id: int, host_info: dict, timestamp):
    db.add(models.SystemInfo(
        server_id=server_id,
        cpu_logic_core=host_info["cpu"]["CPU_logical_core"],
        cpu_physic_core=host_info["cpu"]["CPU_physical_core"],
        cpu_percent=host_info["cpu"]["CPU_percent"],
        cpu_core_usage=host_info["cpu"]["core_usage"],
        mem_total=host_info["memory"]["total_mem_GB"],
        mem_used=host_info["memory"]["used_mem_GB"],
        mem_percent=host_info["memory"]["mem_percent"],
        disk_read_mb=host_info["disk"]["read_MB"],
        disk_write_mb=host_info["disk"]["write_MB"],
        disk_total=host_info["disk"]["usage"]["total_GB"],
        disk_used=host_info["disk"]["usage"]["used_GB"],
        disk_percent=host_info["disk"]["usage"]["percent"],
        net_recv_data_mb=host_info["network"]["recv_data_MB"],
        net_send_data_mb=host_info["network"]["sent_data_MB"],
        net_recv_packets=host_info["network"]["recv_packets"],
        net_send_packets=host_info["network"]["sent_packets"],
        net_recv_err=host_info["network"]["recv_err"],
        net_send_err=host_info["network"]["sent_err"],
        uptime=host_info["uptime"],
        load_avg=host_info["load_avg"],
        timestamp=timestamp
    ))

def _handle_containers(db: Session, server_id: int, containers: list[dict]):
    existing_containers = {
        c.name: c for c in 
        db.query(models.Container).filter(models.Container.host_server == server_id)
    }
    
    active_container_names = set()
    
    for container in containers:
        if not container["namespace"]["mnt"] or not container["namespace"]["pid"]:
            continue
            
        active_container_names.add(container["container_name"])
        container_info = existing_containers.get(container["container_name"])
        
        if not container_info:
            container_info = _create_container(db, server_id, container)
            
        _update_container_ids(db, container_info, container)
        _add_container_stats(db, container_info.id, container)
    
    # Mark removed containers
    removed_names = set(existing_containers.keys()) - active_container_names
    if removed_names:
        db.query(models.Container)\
          .filter(models.Container.name.in_(removed_names))\
          .update({"removed_at": datetime.now()})
          
    db.query(models.Container)\
        .filter(models.Container.name.in_(active_container_names))\
        .update({"removed_at": None})
    
def _create_container(db: Session, server_id: int, container):
    container_info = models.Container(
        host_server=server_id,
        runtime=container["runtime"],
        name=container["container_name"]
    )
    db.add(container_info)
    db.flush()
    return container_info

def _update_container_ids(db: Session, container_info, container):
    # Check if the record already exists
    existing = db.query(models.InternalContainerId).filter(
        models.InternalContainerId.container_idx == container_info.id,
        models.InternalContainerId.pid_id == container["namespace"]["pid"],
        models.InternalContainerId.mnt_id == container["namespace"]["mnt"],
        models.InternalContainerId.cgroup_id == container["cgroup_id"]
    ).first()

    # Add new record if not existing or update if necessary
    if not existing:
        latest_id = db.query(models.InternalContainerId)\
                     .filter(models.InternalContainerId.container_idx == container_info.id)\
                     .order_by(models.InternalContainerId.reg_time.desc())\
                     .first()
                     
        if _should_update_container_id(latest_id, container):
            db.add(models.InternalContainerId(
                container_idx=container_info.id,
                container_id=container["container_id"],
                pid_id=container["namespace"]["pid"],
                mnt_id=container["namespace"]["mnt"],
                cgroup_id=container["cgroup_id"],
                image=container["image"]
            ))

def _should_update_container_id(latest_id, container):
    return not latest_id or \
           latest_id.pid_id != container["namespace"]["pid"] or \
           latest_id.mnt_id != container["namespace"]["mnt"] or \
           latest_id.cgroup_id != container["cgroup_id"]

def _add_container_stats(db: Session, container_id: int, container):
    db.add(models.ContainerSysInfo(
        container_id=container_id,
        cpu_kernel=container["stats"]["cpu"]["kernel_usage"],
        cpu_user=container["stats"]["cpu"]["user_usage"],
        cpu_percent=container["stats"]["cpu"]["usage_percent"],
        cpu_online=container["stats"]["cpu"]["online_cpus"],
        disk_read_mb=container["stats"]["io"]["read_mb"],
        disk_write_mb=container["stats"]["io"]["write_mb"],
        mem_limit=container["stats"]["memory"]["limit_mb"],
        mem_usage=container["stats"]["memory"]["usage_mb"],
        mem_percent=container["stats"]["memory"]["usage_percent"],
        net_recv_mb=container["stats"]["network"]["rx_mb"],
        net_send_mb=container["stats"]["network"]["tx_mb"],
        net_recv_packets=container["stats"]["network"]["rx_packets"],
        net_send_packets=container["stats"]["network"]["tx_packets"],
        ip=container["ip"],
        proc_cnt=container["stats"]["proc_cnt"]
    ))