from fastapi import FastAPI
import uvicorn
import psycopg2
import urllib.request, json

app = FastAPI()

@app.get("/seats")
async def seats(order_id, n_seats):

   print(order_id, n_seats)

   conn = psycopg2.connect(
        database="seats", user='postgres', password='postgres', host='127.0.0.1', port= '5432'
   )
   
   cur = conn.cursor()

   # 50 is the price of the seat -- this is hardcoded for educational purposes
   cur.execute("INSERT INTO public.\"Seat\" (\"OrderID\", \"Total\", \"Status\") VALUES (%s, %s, %s)", (order_id, n_seats, "SUCCESS"))

   conn.commit()

   cur.close()
   conn.close()

   # DO NOT CHANGE THIS: Lets assume that this send to a kafka and we dont know if fails or not
   try:
      send_seats(order_id)
   except Exception as e:
      print(e)

   return {"Result": "Success"}

def send_seats(order_id):
   print(order_id)
   url = "http://localhost:7003/notification?order_id={}".format(order_id)

   response = urllib.request.urlopen(url)
   data = response.read()
   print(data)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=7002)


# To excecute: python3 -m uvicorn main:app --reload --port 7000