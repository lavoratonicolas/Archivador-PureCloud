from types import NoneType
import PureCloudPlatformClientV2
from PureCloudPlatformClientV2.rest import ApiException
from pprint import pprint
import datetime as dt
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from get_token import get_token
import os
import ctypes
import sys

log_path = "logs\\"
log_name = 'sc_ar.log'
police_create = "2022-07-30T00:00:00.000Z"
date_to_archive = "2027-11-14T00:00:00.000Z"
page_size = 100
page_number = 1
increment_days = 1
total_records = 0
days_to_archive_screen = 90
days_to_archive_audio = 90
max_days_to_archive = 30
total_arvhicadas = 0
max_thread = 3
archive_ratio_per_minute = 110
no_archive_ratio_per_minute = 280

start_date = "2020-07-31T00:00:00.000Z"
end_date = "2020-08-01T00:00:00.000Z"

token = ''


def fget_toke():
    global token
    token_aux = 0
    while token_aux < 3:
        token = get_token()
        if token == None:
            token_aux = token_aux + 1
            print("No se pudo obtener el token")
        else:
            token_aux = 4


def get_query(fbody):
    global token
    global log_name
    global log_path

    PureCloudPlatformClientV2.configuration.access_token = token

    api_instance = PureCloudPlatformClientV2.ConversationsApi()

    error_loop = 0
    api_response = 0
    while error_loop < 3:
        try:

            api_response = api_instance.post_analytics_conversations_details_query(
                fbody)

            error_loop = 4
        except ApiException as e:
            print("Exception when calling PostAnalyticsConversationsDetailsQueryRequest->post_analytics_conversations_details_query: %s\n" % e)
            log_name = create_new_log(log_path, log_name)
            with open(log_path + log_name, '+a') as f:
                f.write(str(dt.datetime.fromtimestamp(time.time())) + " ---> " +
                        "Exception when calling PostAnalyticsConversationsDetailsQueryRequest->post_analytics_conversations_details_query: %s\n" % e)
                f.write("\n")
                f.close()
            fget_toke()
            error_loop = error_loop + 1
            time.sleep(5)

    if error_loop == 4:
        return api_response
    else:
        return None


def get_recording(fconversation_id):
    global token
    global log_name
    global log_path

    PureCloudPlatformClientV2.configuration.access_token = token

    api_instance = PureCloudPlatformClientV2.RecordingApi()
    conversation_id = fconversation_id

    error_loop = 0
    api_response = 0
    while error_loop < 3:
        try:

            var_aux = 0
            while (var_aux < 16):
                api_response = api_instance.get_conversation_recordingmetadata(
                    conversation_id)
                if len(api_response) == 0:
                    time.sleep(2)
                    var_aux = var_aux + 1
                else:
                    var_aux = 17
            error_loop = 4
        except ApiException as e:
            print(
                "Exception when calling GetConversationRecordingsRequest->get_conversation_recordings: %s\n" % e)
            log_name = create_new_log(log_path, log_name)
            with open(log_path + log_name, '+a') as f:
                f.write(str(dt.datetime.fromtimestamp(time.time())) + " <--- " + str(conversation_id) + " ---> " +
                        "Exception when calling GetConversationRecordingsRequest->get_conversation_recordings: %s\n" % e)
                f.write("\n")
                f.close()
            fget_toke()
            error_loop = error_loop + 1
            time.sleep(5)

    if error_loop == 4:
        return api_response
    else:
        return None


def calculate_archive_date(recoerding_json, audio_archive_days, screen_archive_days, conv_start):
    global log_path
    global log_name
    global police_create
    global max_days_to_archive
    date_to_archive = 0
    or_date = conv_start

    for x in recoerding_json:
        if x.file_state != 'ARCHIVED' and x.file_state != 'DELETED' and x.archive_date == None:
            date_to_archive = -1
            if type(conv_start) != NoneType and x.media == 'audio':
                when_archive = conv_start + \
                    dt.timedelta(days=audio_archive_days)
                if (dt.datetime.fromtimestamp(time.time()) - when_archive.replace(tzinfo=None)).days > max_days_to_archive:
                    date_to_archive = epoch_to_iso(
                        time.time() - 2 * 24 * 60 * 60)
                else:
                    date_to_archive = epoch_to_iso(when_archive.timestamp())
            elif type(conv_start) != NoneType and x.media == 'screen':
                when_archive = conv_start + \
                    dt.timedelta(days=screen_archive_days)
                if (dt.datetime.fromtimestamp(time.time()) - when_archive.replace(tzinfo=None)).days > max_days_to_archive:
                    date_to_archive = epoch_to_iso(
                        time.time() - 2 * 24 * 60 * 60)
                else:
                    date_to_archive = epoch_to_iso(when_archive.timestamp())
            else:
                date_to_archive = epoch_to_iso(time.time() - 2 * 24 * 60 * 60)
        if date_to_archive != 0 and date_to_archive != -1:
            return x.conversation_id, x.id, date_to_archive
        elif date_to_archive == 0:
            log_name = create_new_log(log_path, log_name)
            with open(log_path + log_name, '+a') as f:
                f.write(str(dt.datetime.fromtimestamp(time.time())) + ' <--- ' + str(or_date) + " ---> " + str(x.conversation_id) +
                        " Audio no disponible o ya tiene fecha de archivado")
                f.write("\n")
                f.close()
        elif date_to_archive == -1:
            log_name = create_new_log(log_path, log_name)
            with open(log_path + log_name, '+a') as f:
                f.write(str(dt.datetime.fromtimestamp(time.time())) + ' <--- ' + str(or_date) + " ---> " + str(x.conversation_id) +
                        " No se tiene fecha de creación o fue creado luego de activar las políticas")
                f.write("\n")
                f.close()
        else:
            log_name = create_new_log(log_path, log_name)
            with open(log_path + log_name, '+a') as f:
                f.write(str(dt.datetime.fromtimestamp(time.time())) + ' <--- ' + str(or_date) + " ---> " + str(x.conversation_id) +
                        " Error no contemplado")
                f.write("\n")
            f.close()
        return None


def update_archive_date(conv_id, record_id, arch_date, ori_date):
    global log_path
    global log_name
    global token

    PureCloudPlatformClientV2.configuration.access_token = token

    api_instance = PureCloudPlatformClientV2.RecordingApi()
    conversation_id = conv_id
    recording_id = record_id

    body = {
        "archiveDate": arch_date
    }

    error_loop = 0
    while error_loop < 3:
        try:

            api_response = api_instance.put_conversation_recording(
                conversation_id, recording_id, body)

            log_name = create_new_log(log_path, log_name)
            with open(log_path + log_name, '+a') as f:
                f.write(str(dt.datetime.fromtimestamp(time.time())) + ' <--- ' + str(ori_date) + " ---> " + str(api_response.conversation_id) + " > " + str(api_response.id) +
                        " se actualizo el archivado para el " + str(api_response.archive_date))
                f.write("\n")
                f.close()
            global total_arvhicadas
            total_arvhicadas = total_arvhicadas + 1
            error_loop = 4
        except ApiException as e:
            print(
                "Exception when calling PutConversationRecordingRequest->put_conversation_recording: %s\n" % e)
            log_name = create_new_log(log_path, log_name)
            with open(log_path + log_name, '+a') as f:
                f.write(str(dt.datetime.fromtimestamp(time.time())) + " <--- " + str(conversation_id) + " ---> " +
                        "Exception when calling PutConversationRecordingRequest->put_conversation_recording: %s\n" % e)
                f.write("\n")
                f.close()
            fget_toke()
            error_loop = error_loop + 1
            time.sleep(5)


def iso_to_epoch(iso_time):
    return dt.datetime.strptime(iso_time, '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()


def epoch_to_iso(epoch_time):
    return dt.datetime.fromtimestamp(epoch_time).isoformat('T', "milliseconds") + 'Z'


def day_add(iso_time, days):
    return epoch_to_iso(iso_to_epoch(iso_time) + days * 24 * 60 * 60)


def update_body(fPage_size, fPage_number, fInterval):
    body = {
        "interval": fInterval,
        "order": "asc",
        "startOfDayIntervalMatching": True,
        "orderBy": "conversationStart",
        "paging": {
            "pageSize": fPage_size,
            "pageNumber": fPage_number
        },
        "segmentFilters": [
            {
                "type": "and",
                "predicates": [
                    {
                        "type": "dimension",
                        "dimension": "recording",
                        "operator": "exists",
                        "value": None
                    },
                    {
                        "type": "dimension",
                        "dimension": "queueId",
                        "operator": "notExists",
                        "value": None
                    }
                ]
            }
        ],
        "conversationFilters": [
            {
                "type": "and",
                "predicates": [
                    {
                        "type": "dimension",
                        "dimension": "originatingDirection",
                        "operator": "matches",
                        "value": "inbound"
                    }
                ]
            }
        ]
    }
    return body


def split_by_pages(ftotal_hits, fpage_size):
    global page_size
    ftotal_pages = ftotal_hits / fpage_size
    ftotal_pages = int(ftotal_pages)
    if ftotal_hits % page_size != 0:
        ftotal_pages += 1
    return ftotal_pages


def split_thread(conversation):
    global days_to_archive_audio
    global days_to_archive_screen
    return_get_record = get_recording(conversation.conversation_id)
    if return_get_record != None:
        return_calc = calculate_archive_date(
            return_get_record, days_to_archive_audio, days_to_archive_screen, conversation.conversation_start)
        if return_calc != None:
            update_archive_date(
                return_calc[0], return_calc[1], return_calc[2], conversation.conversation_start)


def create_new_log(path, current_log_name):
    log_aux = 0
    if os.path.exists(path):
        pass
    else:
        os.mkdir(path)

    if os.path.exists(path + current_log_name):
        size = os.path.getsize(path + current_log_name)
        if ((size/1024)/1024) > 100:
            pass
        else:
            log_aux = 1
    if log_aux == 0:
        now = dt.datetime.now()
        dt_string = now.strftime("%Y%m%d_%H%M%S_%f")
        current_log_name = "sc_ar_" + str(dt_string) + ".log"
    return current_log_name


def disable_console_input():
    sys.stdin.read(0)
    print("Standard input deshabilitado")
    kernel32 = ctypes.windll.kernel32
    kernel32.SetConsoleMode(kernel32.GetStdHandle(-10), 144)

# https://stackoverflow.com/questions/37500076/how-to-enable-windows-console-quickedit-mode-from-python
# -10 is input handle => STD_INPUT_HANDLE (DWORD) -10 | https://docs.microsoft.com/en-us/windows/console/getstdhandle
# default = (0x4|0x80|0x20|0x2|0x10|0x1|0x40|0x200)
# 0x40 is quick edit, #0x20 is insert mode
# 0x8 is disabled by default
# https://docs.microsoft.com/en-us/windows/console/setconsolemode


def enable_console_input():
    sys.stdin.read(1)
    print("Standard input habilitado")
    kernel32 = ctypes.windll.kernel32
    kernel32.SetConsoleMode(kernel32.GetStdHandle(-10),
                            (0x4 | 0x80 | 0x20 | 0x2 | 0x10 | 0x1 | 0x40 | 0x100))


def main(fstart_date, fend_date, fincrement_days, ftotal_records, fpage_size):
    global log_path
    global log_name
    global page_size
    global page_number

    print("Archivator2000" + "\n")
    print("Ejemplo de fecha:" + "\n" + "1987-06-24T00:00:00.000Z" + "\n")
    fstart_date = str(input("Ingrese fecha de inicio: "))
    fend_date = str(input("Ingrese fecha de fin: "))
    disable_console_input()
    print("\n")

    fstart_date_aux = fstart_date
    fstart_date_aux2 = fstart_date

    log_name = create_new_log(log_path, log_name)
    with open(log_path + log_name, '+a') as h:
        h.close()

    while fstart_date_aux < fend_date:
        fget_toke()
        fstart_date_aux = day_add(fstart_date, fincrement_days)
        print(str(fstart_date) + "/" + str(fstart_date_aux))

        fquery_return = get_query(update_body(
            page_size, page_number, str(fstart_date) + "/" + str(fstart_date_aux)))
        ftotal_records += fquery_return.total_hits
        print(str(fquery_return.total_hits) + ' interacciones. Demora entre ' + time.strftime('%H:%M:%S', time.gmtime(fquery_return.total_hits*60 /
              no_archive_ratio_per_minute)) + ' y ' + time.strftime('%H:%M:%S', time.gmtime(fquery_return.total_hits*60/archive_ratio_per_minute)))

        ftotal_pages = 1
        if fquery_return.total_hits > fpage_size:
            ftotal_pages = split_by_pages(fquery_return.total_hits, fpage_size)

        for x in range(page_number, ftotal_pages+1):
            fquery_return = get_query(update_body(
                page_size, x, str(fstart_date) + "/" + str(fstart_date_aux)))
            if fquery_return.total_hits > 0:

                global max_thread
                with ThreadPoolExecutor(max_thread) as executor:
                    futures = [executor.submit(
                        split_thread, conversation) for conversation in fquery_return.conversations]
                    wait(futures)

            print("Pagina " + str(x) + " de " + str(ftotal_pages))

        fstart_date = day_add(fstart_date, fincrement_days)

    global total_arvhicadas
    print('-----------------------------------------------------')
    print(str(total_arvhicadas) + ' grabaciones archivadas, desde ' +
          str(fstart_date_aux2) + ' hasta ' + str(fend_date))


main(start_date, end_date, increment_days, total_records, page_size)
enable_console_input()
input("Presione una tecla para finalizar")
