
import os
import pathlib
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

script_dir = pathlib.Path(__file__).parent

env_path = (script_dir / ".." / "..").resolve() / ".env"

load_dotenv(env_path)

def get_db_session():
    DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

    # Cria o engine
    engine = create_engine(DATABASE_URL)

    # Cria uma sessão
    Session = sessionmaker(bind=engine)
    session = Session()
    
    return session
    
def update_pipeline_status(pipeline_id, status, task_name, step, duration=None, notes=None):
    
    session = get_db_session()
    
    try:
        query = text("""
                        CALL update_pipeline_status(
                            :pipeline_id,
                            :status,
                            :task_name,
                            :duration,
                            :notes,
                            :step
                        )
                     """)
        
        session.execute(query, {
            'pipeline_id': pipeline_id,
            'status': status,
            'task_name': task_name,
            'duration': duration,
            'notes': notes,
            'step': step
        })
        session.commit()
        print(f"Log registrado com sucesso: ID {pipeline_id}")
    except Exception as e:
        session.rollback()
        print(f"Erro ao registrar log: {e}")
    finally:
        session.close()