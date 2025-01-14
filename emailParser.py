import os
import shutil as sh
import pandas as pd
import re
import traceback
import time
from datetime import datetime
from requests import request
from imap_tools import MailBox, AND, OR

mail_username = str(os.getenv('mail.mailUsername'))
mail_password = str(os.getenv('mail.mailPassword'))
server = str(os.getenv('mail.smtpHost'))
input_dir = str(os.getenv('inputDataDir'))
energy_history_dir = input_dir + '/energyHistory/'
weather_dir = input_dir + '/weather/'
weather_dir_temp = input_dir + '/weather_temp/'
message_from = str(os.getenv('mailFrom'))
report_format = '%Y-%m-%d %H:%M'
griddly_format = '%d/%m/%Y %H:%M'
weather_format = '%d%m%y %H%M'
env_url = str(os.getenv('griddly.env_url'))
folder='INBOX'
errors = {}


def retrieve_access_token(host, user, password):
    url = "https://" + str(host + "/griddly/authorize")
    payload = json.dumps({"login": user, "password": password})
    headers = {'accept': '*/*', 'Content-Type': 'application/json'}
    access_token = requests.request("POST", url, headers=headers, data=payload).json()["id_token"]
    return f"Bearer {access_token}"


def get_weather_records(time_from, time_to, location):
    url = "https://" + str(env_url + "/griddly/griddly/getWeatherForecast")
    headers = {
        'accept': '*/*',
        'Authorization': retrieve_access_token(host=env_url, user=username, password=password),
        'Content-Type': 'application/json'
    }
    params = {
        'from': time_from,
        'to': time_to,
        'location': location
    }
    return request("GET", url, headers=headers, data={}, params=params)


def save_file(file_name, payload, to_dir):
    file_path = os.path.join(to_dir, file_name)
    fp = open(file_path, 'wb')
    fp.write(payload)
    fp.close()
    print(f'File {file_name} was successfully moved to {to_dir}.')


def energy_history(file_name, payload):
    save_file(file_name, payload, energy_history_dir)


def irradiance(file_name, payload):
    datetime_header = 'dateTime'
    irradiance_header = 'solarIrradiance'
    headers = {
        'time': datetime_header,
        'tempC': 'temperature',
        'humidity': 'humidity',
        'windSpeed': 'windSpeed',
        'windDirection': 'windDirection',
        'cloudCover': 'cloudCover',
        'percipMM': 'precipitation',
        'solarIrradiance': irradiance_header
    }
    save_file(file_name, payload, weather_dir_temp)
    file_from = weather_dir_temp + file_name
    csv = pd.read_excel(file_from, skiprows=1)
    csv['Date'] = csv['Date'] + ' ' + csv['Time']
    csv['Date'] = pd.to_datetime(csv['Date'], format=griddly_format)
    csv = csv.drop(columns='Time')
    csv.columns = csv.columns.str.lower()
    csv.columns = csv.columns.str.replace('-', '')
    csv = csv.rename(columns={'date': datetime_header})
    from_time = datetime.strftime(min(csv[datetime_header]), report_format)
    to_time = datetime.strftime(max(csv[datetime_header]), report_format)
    frames = []
    for header in csv.keys()[1:]:
        location = header.lower().replace('-', '')
        response = get_weather_records(from_time, to_time, location)
        if response.status_code == 200:
            response = response.json()
            data = pd.json_normalize(response) if len(response) > 0 else pd.DataFrame(columns=headers.keys())
            data['time'] = pd.to_datetime(data['time'], unit='s')
            data.insert(loc=0, column='city', value=location)
            data = data.drop(columns=['issueTime', 'location', 'feelsLikeC', 'heatIndexC', 'snowLevel', 'weatherDesc',
                                      'skyCondition'], errors='ignore')
            data = data.rename(columns=headers)
            frames.append(data)
        else:
            print(f"Couldn't get weather records for {location} from {from_time} to {to_time}")
    result = pd.concat(frames, ignore_index=True)
    res = pd.DataFrame()
    for header in csv.keys()[1:]:
        left = csv[[datetime_header, header]].set_index(datetime_header)[header]
        left.name = 'solarLeft'
        right = result[result['city'] == header].set_index(datetime_header)
        right = right.join(left, how='right')
        right[irradiance_header] = right['solarLeft']
        right = right.drop(columns=['solarLeft', 'city'])
        right = right.reset_index()
        right.insert(loc=0, column='city', value=header)
        right[datetime_header] = right[datetime_header].dt.strftime(griddly_format)
        res = pd.concat([res, right])
    res.to_csv(file_from, index=False)
    sh.move(file_from, weather_dir + file_name)


def weather(file_name, payload):
    headers = {
        'DATE': 'dateTime',
        'T': 'temperature',
        'RH': 'humidity',
        'WS': 'windSpeed',
        'WD': 'windDirection',
        'CLM': 'cloudCover',
        'RRR': 'precipitation',
        'city': 'city'
    }
    save_file(file_name, payload, weather_dir_temp)
    file_from = weather_dir_temp + file_name
    result1 = open(file_from, 'r')
    lines = result1.readlines()
    file_name = file_name + '.csv'
    file_to = weather_dir_temp + file_name
    result2 = open(file_to, 'w')
    record = None
    city = ''
    header = ''
    for line in lines:
        if line.startswith('3'):
            header = re.sub('\s+', ',', line.strip()).replace('3', 'city')
    result2.write(header)
    result2.write('\r')
    for line in lines:
        if line.startswith('2'):
            city = line.replace('2', '').strip().replace('1_', '') + ','
        elif line.startswith(' ') and not line.strip().startswith('-'):
            record = re.sub('\s+', ',', line.strip())
            record = city.lower() + str(record)
        if record is not None:
            result2.write(record)
            result2.write('\r')
    result1.close()
    result2.close()
    data = pd.read_csv(file_to, dtype=str)
    data['DATE'] = data['DATE'] + ' ' + data['TIME']
    data['DATE'] = pd.to_datetime(data['DATE'], format=weather_format).dt.strftime(griddly_format)
    data = data.drop(columns=['TIME', 'TW', 'HS', 'CS'])
    data = data.rename(columns=headers)
    data.to_csv(file_to, index=False)
    sh.move(file_to, weather_dir + file_name)
    os.remove(file_from)


def weather_observed(file_name, payload):
    headers = {
        'DATE': 'dateTime',
        'T': 'temperature',
        'RH': 'humidity',
        'WS': 'windSpeed',
        'WD': 'windDirection',
        'RAD': 'solarIrradiance',
        'RRR': 'precipitation',
        'city': 'city'
    }
    save_file(file_name, payload, weather_dir_temp)
    file_from = weather_dir_temp + file_name
    result1 = open(file_from, 'r')
    lines = result1.readlines()
    file_name = file_name + '.csv'
    file_to = weather_dir_temp + file_name
    result2 = open(file_to, 'w')
    record = None
    city = ''
    header = ''
    for line in lines:
        if line.startswith('3'):
            header = re.sub('\s+', ',', line.strip()).replace('3', 'city')
    result2.write(header)
    result2.write('\r')
    for line in lines:
        if line.startswith('2'):
            city = line.replace('2', '').strip().replace('1_', '') + ','
        elif line.startswith(' ') and not line.strip().startswith('-'):
            record = re.sub('\s+', ',', line.strip())
            record = city.lower() + str(record)
        if record is not None:
            result2.write(record)
            result2.write('\r')
    result1.close()
    result2.close()
    float_headers = ['temperature', 'humidity', 'windSpeed', 'windDirection', 'solarIrradiance', 'precipitation']
    data = pd.read_csv(file_to, dtype=str)
    data['DATE'] = data['DATE'] + ' ' + data['TIME']
    data['DATE'] = pd.to_datetime(data['DATE'], format=weather_format, utc=False)
    data = data.drop(columns=['TIME', 'TW', 'HS', 'CS'])
    data = data.rename(columns=headers)
    data[float_headers] = data[float_headers].astype(float)
    data = data.replace('_', '', regex=True)
    data = data.groupby(['city', 'dateTime']).mean().reset_index()
    data['dateTime'] = data['dateTime'].dt.strftime(griddly_format)
    data['city'] = data['city'] + '_o'
    data.to_csv(file_to, index=False)
    sh.move(file_to, weather_dir + file_name)
    os.remove(file_from)


processors = {'xlsx': energy_history, 'csv': irradiance, 'sn3': weather, 'sn1': weather_observed}


def parse_email():
    mailbox = MailBox(server).login(username=mail_username, password=mail_password, initial_folder='INBOX')
    mails = mailbox.fetch(AND(OR(from_=message_from), seen=True))
    mails = sorted(mails, key=lambda mail: mail.date)
    for msg in mails:
        print(msg.from_ + msg.date_str)
        for attr in msg.attachments:
            file_name = attr.filename
            ext = file_name.split('.')[-1].lower()
            if ext in processors.keys():
                try:
                    processors[ext](file_name, attr.payload)
                except:
                    errors.update({file_name: traceback.format_exc()})
        time.sleep(5)


parse_email()
if len(errors) > 0:
    for error in errors.keys():
        print(f"Couldn't process {error}:")
        print(errors[error])
    exit(1)
