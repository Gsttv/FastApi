import time
from fastapi import FastAPI, UploadFile, File, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Float,text
import pandas as pd
import shutil
import re
import numpy as np
import logging
import asyncio
import concurrent.futures

app = FastAPI()

# Configuração do logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

# Configuração do banco de dados (ajuste conforme necessário)
DATABASE_URL = 'postgresql+asyncpg://postgres:mel3228303@localhost/postgres'
engine = create_async_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=engine, class_=AsyncSession
)
Base = declarative_base()

# Modelo de dados para itens
class Item(Base):
    __tablename__ = "tb_insumos"
    id = Column(Integer, primary_key=True, index=True)
    codigo = Column(Integer, unique=True)
    descricao = Column(String, index=True)
    unidade_medida = Column(String)
    origem_preco = Column(String)
    preco_mediano = Column(Float)

# Modelo de dados para serviços
class Servico(Base):
    __tablename__ = "tb_servicos"
    id = Column(Integer, primary_key=True, index=True)
    codigo = Column(Integer, unique=True)
    descricao = Column(String, index=True)
    unidade = Column(String)
    custo_total = Column(Float)

# Inicializar o banco de dados
@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def clean_price(price):
    if pd.isna(price):
        return np.nan
    price = str(price)
    price = re.sub(r'[^\d,]', '', price)
    price = price.replace(',', '.')
    return float(price)

def clean_cost(cost):
    if pd.isna(cost):
        return np.nan
    cost = str(cost)
    cost = re.sub(r'[^\d,]', '', cost)
    cost = cost.replace(',', '.')
    return float(cost)

async def process_item_chunk(chunk):
    async with SessionLocal() as db:
        # Garantir que as colunas estão corretas
        if 'codigo' not in chunk.columns:
            chunk.columns = ["codigo", "descricao", "unidade_medida", "origem_preco", "preco_mediano"]

        chunk = chunk.dropna(subset=['codigo'])
        chunk['codigo'] = chunk['codigo'].apply(lambda x: str(x).split('.')[0])
        chunk = chunk[chunk['codigo'].apply(lambda x: x.isdigit())]
        chunk['preco_mediano'] = chunk['preco_mediano'].apply(clean_price)

        items_to_add = []
        item_count = 0
        for _, row in chunk.iterrows():
            existing_item = await db.execute(text("SELECT 1 FROM tb_insumos WHERE codigo = :codigo"), {'codigo': int(row['codigo'])})
            if existing_item.fetchone() is None:
                item = Item(
                    codigo=int(row['codigo']),
                    descricao=row['descricao'],
                    unidade_medida=row['unidade_medida'],
                    origem_preco=row['origem_preco'],
                    preco_mediano=row['preco_mediano']
                )
                items_to_add.append(item)
                item_count += 1
        db.add_all(items_to_add)
        await db.commit()
        return item_count

async def process_item_data(file_path: str):
    start_time = time.time()
    item_count = 0

    df = pd.read_excel(file_path, header=None, skiprows=6)
    chunk_size = 1000
    chunks = np.array_split(df, len(df) // chunk_size + 1)

    tasks = []
    for chunk in chunks:
        tasks.append(process_item_chunk(chunk))

    for result in await asyncio.gather(*tasks):
        item_count += result

    end_time = time.time()
    duration = end_time - start_time
    print(f"Processamento de itens finalizado. {item_count} itens foram cadastrados em {duration:.2f} segundos.")

async def process_servico_chunk(chunk):
    async with SessionLocal() as db:
        chunk = chunk.dropna(subset=['codigo'])
        chunk['codigo'] = chunk['codigo'].apply(lambda x: str(x).split('.')[0])
        chunk = chunk[chunk['codigo'].apply(lambda x: x.isdigit())]
        chunk['custo_total'] = chunk['custo_total'].apply(clean_cost)

        servicos_to_add = []
        servico_count = 0
        for _, row in chunk.iterrows():
            existing_servico = await db.execute(text("SELECT 1 FROM tb_servicos WHERE codigo = :codigo"), {'codigo': int(row['codigo'])})
            if existing_servico.fetchone() is None:
                servico = Servico(
                    codigo=int(row['codigo']),
                    descricao=row['descricao'],
                    unidade=row['unidade'],
                    custo_total=row['custo_total']
                )
                servicos_to_add.append(servico)
                servico_count += 1
        db.add_all(servicos_to_add)
        await db.commit()
        return servico_count

async def process_servico_data(file_path: str):
    start_time = time.time()
    servico_count = 0

    df = pd.read_excel(file_path, header=None)
    def normalize_column_name(col_name):
        return col_name.strip().replace(" ", "").lower()

    header_row = None
    for i, row in df.iterrows():
        if i > 50:
            break
        normalized_row = [normalize_column_name(str(col)) for col in row.values]
        if all(col in normalized_row for col in ["codigodacomposicao", "descricaodacomposicao", "unidade", "custototal"]):
            header_row = i
            break

    if header_row is None:
        raise ValueError("Cabeçalho esperado não encontrado na planilha.")

    df = pd.read_excel(file_path, header=header_row)
    df.columns = [normalize_column_name(col) for col in df.columns]
    required_columns = ["codigodacomposicao", "descricaodacomposicao", "unidade", "custototal"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"As colunas esperadas estão faltando: {missing_columns}")

    df = df[required_columns]
    df.columns = ["codigo", "descricao", "unidade", "custo_total"]
    df['codigo'] = df['codigo'].astype(str)
    df['codigo'] = df['codigo'].apply(lambda x: x.split('.')[0])
    df = df.drop_duplicates(subset=['codigo'], keep='first')
    df = df[df['codigo'].apply(lambda x: x.isdigit())]

    chunk_size = 1000
    chunks = np.array_split(df, len(df) // chunk_size + 1)

    tasks = []
    for chunk in chunks:
        tasks.append(process_servico_chunk(chunk))

    for result in await asyncio.gather(*tasks):
        servico_count += result

    end_time = time.time()
    duration = end_time - start_time
    print(f"Processamento de serviços finalizado. {servico_count} serviços foram cadastrados em {duration:.2f} segundos.")

# Endpoint de upload com tarefa de background
@app.post("/upload/")
async def upload_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    file_path = f"temp_{file.filename}"
    with open(file_path, "wb+") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Determinar o tipo de planilha com base no nome do arquivo
    if 'composicao' in file.filename.lower() or 'composicoes' in file.filename.lower():
        background_tasks.add_task(process_servico_data, file_path)
    else:
        background_tasks.add_task(process_item_data, file_path)

    return {"message": "File uploaded successfully, processing started."}