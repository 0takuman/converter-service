import pika, sys, os, time

from pymongo import MongoClient
import gridfs

from convert import to_mp3

def main():
    client = MongoClient("host.minikube.internal", 27017)
    db_videos = client.videos
    db_mp3 = client.mp3

    #gridfs
    fs_videos = gridfs.GridFS(db_videos)
    fs_mp3s = gridfs.GridFS(db_mp3s)

    #rabbitmq connection
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
          host="rabbitmq"
        )
    )
    channel = connection.channel()
    channel.basic_consume(
        queue=os.environ.get("VIDEO_QUEUE"),
        on_message_callback=callback
    )
    print("Waiting for messsages. To exit press CTRL+C")

    channel.start_consuming()

def callback(ch, method, properties, body):
    err = to_mp3.start(body, fs_videos, fs_mp3s, ch)
    if err:
        ch.basic_nack(delivery_tag=method.delivery_tag)
    else:
        ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == "main":
    try:
        main()
    except KeyboardInterrupt:
        print("Interupted")
        try:
            sys.exit(0)
        except SystemExit:
            os.exit(0)