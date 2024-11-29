#!/usr/bin/env python3

import argparse
import io
import json
import os
import sys
import gc
import pyarrow as pa
from enum import Enum

import singer
from multiprocessing import get_context
from jsonschema.validators import Draft4Validator
from datetime import datetime
import pyarrow.parquet as pq

from target_s3_parquet import s3
from target_s3_parquet import utils
from multiprocessing import Queue


logger = singer.get_logger('target_s3_parquet')


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()
#
#
# # pylint: disable=too-many-locals,too-many-branches,too-many-statements
# def persist_messages(messages, config, s3_client):
#     state = None
#     schemas = {}
#     key_properties = {}
#     headers = {}
#     validators = {}
#
#     delimiter = config.get('delimiter', ',')
#     quotechar = config.get('quotechar', '"')
#
#     # Use the system specific temp directory if no custom temp_dir provided
#     temp_dir = os.path.expanduser(config.get('temp_dir', tempfile.gettempdir()))
#
#     # Create temp_dir if not exists
#     if temp_dir:
#         os.makedirs(temp_dir, exist_ok=True)
#
#     # dictionary to hold csv filename per stream
#     filenames = {}
#
#     now = datetime.now().strftime('%Y%m%dT%H%M%S')
#
#     for message in messages:
#         try:
#             o = singer.parse_message(message).asdict()
#         except json.decoder.JSONDecodeError:
#             logger.error("Unable to parse:\n{}".format(message))
#             raise
#
#         # logger.info(o)
#
#         message_type = o['type']
#         if message_type == 'RECORD':
#             stream_name = o['stream']
#
#             if stream_name not in schemas:
#                 raise Exception("A record for stream {}"
#                                 "was encountered before a corresponding schema".format(stream_name))
#
#             # Validate record
#             try:
#                 validators[stream_name].validate(utils.float_to_decimal(o['record']))
#             except Exception as ex:
#                 if type(ex).__name__ == "InvalidOperation":
#                     logger.error("Data validation failed and cannot load to destination. \n"
#                                  "'multipleOf' validations that allows long precisions are not supported"
#                                  " (i.e. with 15 digits or more). Try removing 'multipleOf' methods from JSON schema.")
#                     raise ex
#
#             record_to_load = o['record']
#             if config.get('add_metadata_columns'):
#                 record_to_load = utils.add_metadata_values_to_record(o, {})
#             else:
#                 record_to_load = utils.remove_metadata_values_from_record(o)
#
#             if stream_name not in filenames:
#                 filename = os.path.expanduser(os.path.join(temp_dir, stream_name + '-' + now + '.parquet'))
#                 logger.info(filename)
#
#                 filenames[stream_name] = {
#                     'filename': filename,
#                     'target_key': utils.get_target_key(message=o,
#                                                        prefix=config.get('s3_key_prefix', ''),
#                                                        timestamp=now,
#                                                        naming_convention=config.get('naming_convention'))
#
#                 }
#             else:
#                 filename = filenames[stream_name]['filename']
#
#             file_is_empty = (not os.path.isfile(filename)) or os.stat(filename).st_size == 0
#
#             flattened_record = utils.flatten_record(record_to_load)
#
#             if stream_name not in headers and not file_is_empty:
#                 with open(filename, 'r') as csvfile:
#                     reader = csv.reader(csvfile,
#                                         delimiter=delimiter,
#                                         quotechar=quotechar)
#                     first_line = next(reader)
#                     headers[stream_name] = first_line if first_line else flattened_record.keys()
#             else:
#                 headers[stream_name] = flattened_record.keys()
#
#
#             with open(filename, 'ab') as parquetfile:
#                 records = [flattened_record]
#                 # logger.info(records)
#
#                 table = pa.Table.from_pandas(pd.DataFrame(records))
#                 logger.info(table)
#
#                 # Write the table to Parquet (append mode)
#                 pq.write_table(table, parquetfile)
#
#         elif message_type == 'STATE':
#             logger.debug('Setting state to {}'.format(o['value']))
#             state = o['value']
#
#         elif message_type == 'SCHEMA':
#             stream_name = o['stream']
#             schemas[stream_name] = o['schema']
#
#             if config.get('add_metadata_columns'):
#                 schemas[stream_name] = utils.add_metadata_columns_to_schema(o)
#
#             schema = utils.float_to_decimal(o['schema'])
#             validators[stream_name] = Draft7Validator(schema, format_checker=FormatChecker())
#             key_properties[stream_name] = o['key_properties']
#         elif message_type == 'ACTIVATE_VERSION':
#             logger.debug('ACTIVATE_VERSION message')
#         else:
#             logger.warning("Unknown message type {} in message {}".format(o['type'], o))
#
#
#
#     # Upload created CSV files to S3
#     s3.upload_files(iter(filenames.values()), s3_client, config['s3_bucket'], config.get("compression"),
#                     config.get('encryption_type'), config.get('encryption_key'))
#
#     return state

class MessageType(Enum):
    RECORD = 1
    STATE = 2
    SCHEMA = 3
    EOF = 4

def create_dataframe(list_dict):
    fields = set()
    for d in list_dict:
        fields = fields.union(d.keys())
    dataframe = pa.table({f: [row.get(f) for row in list_dict] for f in fields})
    return dataframe

def persist_messages(
    messages,
    config,
    s3_client,
    compression_method=None,
    streams_in_separate_folder=False,
    file_size=-1,
):
    # Multiprocessing context
    if sys.platform == "darwin" or sys.platform == 'linux':
        ctx = get_context("fork")
    else:
        ctx = get_context("spawn")

    ## Static information shared among processes
    schemas = {}
    key_properties = {}
    validators = {}
    filepath = {}

    compression_extension = ""
    if compression_method:
        # The target is prepared to accept all the compression methods provided by the pandas module, with the mapping below,
        extension_mapping = {
            "SNAPPY": ".snappy",
            "GZIP": ".gz",
            "BROTLI": ".br",
            "ZSTD": ".zstd",
            "LZ4": ".lz4",
        }
        compression_extension = extension_mapping.get(compression_method.upper())
        if compression_extension is None:
            logger.warning("unsuported compression method.")
            compression_extension = ""
            compression_method = None
    filename_separator = "-"

    ## End of Static information shared among processes

    # Object that signals shutdown
    _break_object = object()

    def producer(message_buffer: io.TextIOWrapper, w_queue: Queue):
        state = None
        try:
            for message in message_buffer:
                logger.debug(f"target-parquet got message: {message}")
                try:
                    message = singer.parse_message(message).asdict()
                except json.decoder.JSONDecodeError:
                    raise Exception("Unable to parse:\n{}".format(message))

                message_type = message["type"]
                if message_type == "RECORD":
                    if message["stream"] not in schemas:
                        raise ValueError(
                            "A record for stream {} was encountered before a corresponding schema".format(
                                message["stream"]
                            )
                        )
                    stream_name = message["stream"]
                    validators[message["stream"]].validate(message["record"])
                    flattened_record = utils.flatten(message["record"])
                    # Once the record is flattenned, it is added to the final record list, which will be stored in the parquet file.
                    w_queue.put((MessageType.RECORD, stream_name, flattened_record))
                    state = None
                elif message_type == "STATE":
                    logger.debug("Setting state to {}".format(message["value"]))
                    state = message["value"]
                elif message_type == "SCHEMA":
                    stream = message["stream"]
                    validators[stream] = Draft4Validator(message["schema"])
                    schemas[stream] = utils.flatten_schema(message["schema"]["properties"])
                    logger.debug(f"Schema: {schemas[stream]}")
                    key_properties[stream] = message["key_properties"]
                    w_queue.put((MessageType.SCHEMA, stream, schemas[stream]))
                else:
                    logger.warning(
                        "Unknown message type {} in message {}".format(
                            message["type"], message
                        )
                    )
            w_queue.put((MessageType.EOF, _break_object, None))
            return state
        except Exception as Err:
            w_queue.put((MessageType.EOF, _break_object, None))
            raise Err

    def write_file(current_stream_name, record):
        # logger.info(record)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S-%f")
        logger.debug(f"Writing files from {current_stream_name} stream")
        dataframe = create_dataframe(record)

        filename = (
            current_stream_name
            + filename_separator
            + timestamp
            + compression_extension
            + ".parquet"
        )
        filepath = config.get('s3_key_prefix') + filename
        buffer = io.BytesIO()
        pq.write_table(dataframe, buffer)
        buffer.seek(0)


        s3_client.upload_fileobj(buffer, config.get("s3_bucket"), filepath)

        del dataframe
        return filepath

    def consumer(receiver):
        files_created = []
        current_stream_name = None
        # records is a list of dictionary of lists of dictionaries that will contain the records that are retrieved from the tap
        records = {}
        schemas = {}

        while True:
            (message_type, stream_name, record) = receiver.get()  # q.get()
            if message_type == MessageType.RECORD:
                if (stream_name != current_stream_name) and (
                    current_stream_name != None
                ):
                    files_created.append(
                        write_file(
                            current_stream_name, records.pop(current_stream_name)
                        )
                    )
                    ## explicit memory management. This can be usefull when working on very large data groups
                    gc.collect()
                current_stream_name = stream_name
                if type(records.get(stream_name)) != list:
                    records[stream_name] = [record]
                else:
                    records[stream_name].append(record)
                    if (file_size > 0) and (not len(records[stream_name]) % file_size):
                        files_created.append(
                            write_file(
                                current_stream_name, records.pop(current_stream_name)
                            )
                        )
                        gc.collect()
            elif message_type == MessageType.SCHEMA:
                schemas[stream_name] = record
            elif message_type == MessageType.EOF:
                files_created.append(
                    write_file(current_stream_name, records.pop(current_stream_name))
                )
                logger.info(f"Wrote {len(files_created)} files")
                logger.debug(f"Wrote {files_created} files")
                break

    q = ctx.Queue()
    t2 = ctx.Process(
        target=consumer,
        args=(q,),
    )
    t2.start()
    state = producer(messages, q)
    t2.join()
    return state

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}

    config_errors = utils.validate_config(config)
    if len(config_errors) > 0:
        logger.error("Invalid configuration:\n   * {}".format('\n   * '.join(config_errors)))
        sys.exit(1)

    s3_client = s3.create_client(config)

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_messages(input_messages,
                             config,
                             s3_client)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
