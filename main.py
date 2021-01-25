# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START gae_python38_render_template]
import datetime

from google.cloud import bigquery
from google.cloud import pubsub_v1
from flask import Flask, render_template
from flask import request
import json

app = Flask(__name__)


@app.route('/')
def root():
    # For the sake of example, use static information to inflate the template.
    # This will be replaced with real information in later steps.
    return render_template('newUser.html')


@app.route('/unsubscribe')
def unsubscribe():
    print("In unsubscribe")
    return render_template('unsubscribe.html')


@app.route('/unsubscribe', methods=['POST'])
def unsubscribeSucces():
    data = request.form
    email = data.__getitem__("email")

    project_id = "group2project"
    topic_id = "unsubscribeTopic"

    publisher = pubsub_v1.PublisherClient()
    # The `topic_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/topics/{topic_id}`
    topic_path = publisher.topic_path(project_id, topic_id)

    # data = "Message number {}".format(n)
    # Data must be a bytestring


    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, email.encode("utf-8"))
    print(future.result())

    return render_template('unsubscribesuccess.html', email=email)


@app.route('/', methods=['POST'])
def newuser():
    print("in new user")
    data = request.form  # a multidict containing POST data
    client = bigquery.Client()

    project_id = "group2project"
    table_id = "group2project.VB_dataset.registered_users"
    date = data.__getitem__("monthyear") + "-01"
    email = data.__getitem__("email")
    print(data)
    print(date)
    if data.__getitem__("fvisatype") == "":
        category = data.__getitem__("evisatype")
        country = data.__getitem__("ecountry")
    else:
        category = data.__getitem__("fvisatype")
        country = data.__getitem__("fcountry")
    rows_to_insert = [
        {u"email": email, u"date": date, u"category": category, "country": country},
    ]

    errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

    return render_template('success.html', email=data.__getitem__("email"))


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    # Flask's development server will automatically serve static files in
    # the "static" directory. See:
    # http://flask.pocoo.org/docs/1.0/quickstart/#static-files. Once deployed,
    # App Engine itself will serve those files as configured in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END gae_python38_render_template]
