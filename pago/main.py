from fastapi import FastAPI
import uvicorn
import psycopg2
import urllib.request, json

app = FastAPI()

@app.get("/payment")
async def payment(order_id, n_seats):

   print(order_id, n_seats)

   conn = psycopg2.connect(
        database="payments", user='postgres', password='postgres', host='127.0.0.1', port= '5432'
   )
   
   cur = conn.cursor()

   # 50 is the price of the seat -- this is hardcoded for educational purposes
   total = int(n_seats) * 50
   print(total)
   cur.execute("INSERT INTO public.\"Payment\" (\"OrderId\", \"Total\", \"Status\") VALUES (%s, %s, %s)", (order_id, total, "SUCCESS"))

   conn.commit()

   cur.close()
   conn.close()
   
   # DO NOT CHANGE THIS: Lets assume that this send to a kafka and we dont know if fails or not
   try:
      send_payment(n_seats, order_id)
   except Exception as e:
      print(e)

   return {"Result": "Success"}

def send_payment(n_seats, order_id):
   print(n_seats, order_id)
   url = "http://localhost:7002/seats?order_id={}&n_seats={}".format(order_id, n_seats)

   response = urllib.request.urlopen(url)
   data = response.read()
   print(data)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=7001)


# To excecute: python3 -m uvicorn main:app --reload --port 7000