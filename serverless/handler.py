import json


def hello(event, context):
    try:
        return dict(
            statusCode=200,
            body="hello"
        )

    except Exception as e:
        return dict(
            statusCode=500,
            body=str(e)
        )
        

# def hello(event, context):
#     body = {
#         "message": "Hello",
#         "input": event
#     }

#     response = {
#         "statusCode": 200,
#         "body": json.dumps(body)
#     }

#     return response

