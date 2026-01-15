import modal

app = modal.App("antigravity-test")

@app.function()
def hello():
    print("Это сообщение напечатано в облаке Modal!")
    return "✅ Modal успешно запущен из Antigravity"

@app.local_entrypoint()
def main():
    result = hello.remote()
    print(result)
