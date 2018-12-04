from celery import Celery

app = Celery(
    'download_carto_chunks', 
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0',
    include=(
        'download_carto_chunks.download_carto_dataset'
    )
)