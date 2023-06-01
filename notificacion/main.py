from fastapi import FastAPI
import uvicorn
import psycopg2
import urllib.request, json

app = FastAPI()

@app.get("/notification")
async def notification(order_id):

   print(order_id)

   conn = psycopg2.connect(
        database="notifications", user='postgres', password='postgres', host='127.0.0.1', port= '5432'
   )
   
   cur = conn.cursor()

   # 50 is the price of the seat -- this is hardcoded for educational purposes
   cur.execute("INSERT INTO public.\"notification\" (\"OrderID\", \"Type\", \"Status\") VALUES (%s, %s, %s)", (order_id, "EMAIL", "SUCCESS"))

   conn.commit()

   cur.close()
   conn.close()

   # DO NOT DELETE: simulating an error in the email service
   raise Exception("Email service is down")

   # return {"Result": "Success"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=7003)


# To excecute: python3 -m uvicorn main:app --reload --port 7000