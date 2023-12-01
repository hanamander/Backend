import uvicorn

if __name__ == "__main__":
    # uvicorn.run("app:app", port=3001, host="0.0.0.0");
    uvicorn.run("app:app", port=3001, host="0.0.0.0", reload=True);
    # uvicorn.run("app:app", port=3001, host="0.0.0.0", reload=True, log_level="critical");