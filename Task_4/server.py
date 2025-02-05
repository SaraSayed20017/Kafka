import os
import sqlite3
import sys
import uuid

from flask import Flask, redirect, render_template_string, request, send_from_directory
from confluent_kafka import Producer
from PIL import Image, ImageDraw, ImageFont

IMAGES_DIR = "images"
MAIN_DB = "main.db"
FONT_PATH = "arial.ttf"  # Path to your font file
WATERMARK_TEXT = "Watermark"  # Default watermark text
FONT_SIZE = 24  # Default font size

app = Flask(__name__)

# Kafka producer configuration
kafka_conf = {
    "bootstrap.servers": "34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094"
}
producer = Producer(kafka_conf)
topic = "SaraSayed_5"

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def get_db_connection():
    conn = sqlite3.connect(MAIN_DB)
    conn.row_factory = sqlite3.Row
    return conn

con = get_db_connection()
con.execute("CREATE TABLE IF NOT EXISTS image(id TEXT, filename TEXT, object TEXT)")
con.close()
if not os.path.exists(IMAGES_DIR):
    os.mkdir(IMAGES_DIR)

def add_watermark(image_path, watermark_text=WATERMARK_TEXT, font_path=FONT_PATH, font_size=FONT_SIZE):
    print("Adding watermark to:", image_path)  # Debug print

    # Open the image
    image = Image.open(image_path)

    # Create a drawing context
    draw = ImageDraw.Draw(image)

    # Get the image size
    width, height = image.size

    # Load a font
    font = ImageFont.truetype(font_path, font_size)

    # Calculate the size of the text
    text_width, text_height = draw.textsize(watermark_text, font=font)

    # Calculate the position of the watermark
    margin = 10
    position = (width - text_width - margin, height - text_height - margin)

    # Add the watermark to the image
    draw.text(position, watermark_text, fill="white", font=font)

    # Save the image with the watermark
    image.save(image_path)


@app.route('/', methods=['GET'])
def index():
    con = get_db_connection()
    cur = con.cursor()
    res = cur.execute("SELECT * FROM image")
    images = res.fetchall()
    con.close()
    return render_template_string("""
<!DOCTYPE html>
<html>
<head>
<style>
.container {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  grid-auto-rows: minmax(100px, auto);
  gap: 20px;
}
img {
   display: block;
   max-width:100%;
   max-height:100%;
   margin-left: auto;
   margin-right: auto;
}
.img {
   height: 270px;
}
.label {
   height: 30px;
  text-align: center;
}
</style>
</head>
<body>
<form method="post" enctype="multipart/form-data">
  <div>
    <label for="file">Choose file to upload</label>
    <input type="file" id="file" name="file" accept="image/x-png,image/gif,image/jpeg" />
  </div>
  <div>
    <button>Submit</button>
  </div>
</form>
<div class="container">
{% for image in images %}
<div>
<div class="img"><img src="/images/{{ image.filename }}"></img></div>
<div class="label">{{ image.object | default('undefined', true) }}</div>
</div>
{% endfor %}
</div>
</body>
</html>
    """, images=images)

@app.route('/images/<path:path>', methods=['GET'])
def image(path):
    return send_from_directory(IMAGES_DIR, path)

@app.route('/object/<id>', methods=['PUT'])
def set_object(id):
    con = get_db_connection()
    cur = con.cursor()
    json = request.json
    object = json['object']
    cur.execute("UPDATE image SET object = ? WHERE id = ?", (object, id))
    con.commit()
    con.close()
    return '{"status": "OK"}'

@app.route('/', methods=['POST'])
def upload_file():
    f = request.files['file']
    ext = f.filename.split('.')[-1]
    id = uuid.uuid4().hex
    filename = "{}.{}".format(id, ext)
    image_path = "{}/{}".format(IMAGES_DIR, filename)
    f.save(image_path)

    # Add watermark to the uploaded image
    add_watermark(image_path)

    con = get_db_connection()
    cur = con.cursor()
    cur.execute("INSERT INTO image (id, filename, object) VALUES (?, ?, ?)", (id, filename, ""))
    con.commit()
    con.close()

    # Produce a message to Kafka
    message = f"Image uploaded: {filename}"
    producer.produce(topic, value=message, callback=delivery_report)
    producer.poll(0)

    return redirect('/')

if __name__ == '__main__':
    app.run(debug=True, port=(int(sys.argv[1]) if len(sys.argv) > 1 else 5000))
