from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.init_db import init_db
from app.routers.uploads import router as uploads_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup():
    init_db()

@app.get("/")
def root():
    return {"message": "Backend is running"}

app.include_router(uploads_router)

# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware
# from app.init_db import init_db
# # 确保这里导入了你的 router
# from app.routers.uploads import router as uploads_router 

# app = FastAPI()

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# @app.on_event("startup")
# def startup():
#     init_db()

# # 根路由
# @app.get("/")
# def root():
#     return {"message": "Backend is running"}

# # --- 关键点在这里 ---
# # 确保这一行是独立运行的，不要和 import 语句贴在一起
# app.include_router(uploads_router)