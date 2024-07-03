import shutil
import re

import pandas as pd
from fastapi import BackgroundTasks, FastAPI, File, UploadFile
from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import numpy as np

app = FastAPI()

# Configuração do banco de dados (ajuste conforme necessário)
DATABASE_URL = 'postgresql+asyncpg://postgres:mel3228303@localhost/postgres'
engine = create_async_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=engine, class_=AsyncSession
)
Base = declarative_base()


# Modelo de dados
class Item(Base):
    __tablename__ = "items"
    id = Column(Integer, primary_key=True, index=True)
    codigo = Column(Integer)
    descricao = Column(String, index=True)
    unidade_medida = Column(String)
    origem_preco = Column(String)
    preco_mediano = Column(Float)


# Função de processamento de dados
def process_and_save_data(file_path: str):
    df = pd.read_excel(file_path, header=None)  # Ler a planilha sem cabeçalhos

    # Identificar a linha onde os dados começam
    start_row = 6  # Dados começam na linha 7 (índice 6)
    df = pd.read_excel(file_path, skiprows=start_row)  # Ler a planilha a partir da linha dos dados

    # Filtrar apenas as colunas desejadas
    df = df.iloc[:, [0, 1, 2, 3, 4]]  # Código, Descrição, Unidade de Medida, Origem do Preço, Preço Mediano
    df.columns = ["codigo", "descricao", "unidade_medida", "origem_preco", "preco_mediano"]

    # Remover linhas onde o código é NaN (possível em linhas vazias ou não relevantes)
    df = df.dropna(subset=['codigo'])
    df = df[df['codigo'].apply(lambda x: str(x).isdigit())]

    # Limpar e converter os valores da coluna 'preco_mediano'
    def clean_price(price):
        if pd.isna(price):
            return np.nan
        # Converter para string para aplicar regex
        price = str(price)
        # Remover caracteres não numéricos, exceto pontos e vírgulas
        price = re.sub(r'[^\d,]', '', price)
        # Substituir vírgulas por pontos
        price = price.replace(',', '.')
        return float(price)

    df['preco_mediano'] = df['preco_mediano'].apply(clean_price)

    # Inserir dados no banco de dados
    session = SessionLocal()
    try:
        for _, row in df.iterrows():
            item = Item(
                codigo=int(row['codigo']),
                descricao=row['descricao'],
                unidade_medida=row['unidade_medida'],
                origem_preco=row['origem_preco'],
                preco_mediano=row['preco_mediano']
            )
            session.add(item)
        session.commit()
    finally:
        session.close()

# Endpoint de upload
@app.post("/upload/")
def upload_file(file: UploadFile = File(...)):
    file_path = f"temp_{file.filename}"
    with open(file_path, "wb+") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Processar e salvar os dados de forma síncrona
    process_and_save_data(file_path)

    return {"message": "File uploaded and processed successfully"}


# Inicializar o banco de dados
@app.on_event('startup')
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
