from datetime import datetime
import logging
import multiprocessing
import os
from queue import Empty
import time

import coreapi
from coreapi import codecs
import openapi_codec

from luigi.task_history import TaskHistory


logger = logging.getLogger('luigi-interface')


class SawyerServerConnection:
    schema = None
    error_logging_disabled = False

    def __init__(self, host, token):
        self.host = host
        self.client = coreapi.Client(
            auth = coreapi.auth.TokenAuthentication(token, scheme='Token'),
            decoders=[openapi_codec.OpenAPICodec(), codecs.JSONCodec()])

    def _log_error(self, msg):
        # Log successive connection errors only once.
        # Otherwise, if server is down, we'll spam the logs once every `timeout` seconds.
        if not self.error_logging_disabled:
            logger.error(msg)
            self.error_logging_disabled = True

    def _log_success(self, msg):
        logger.info(msg)
        self.error_logging_disabled = False

    def post(self, tasks):
        if not self.schema:
            try:
                self.schema = self.client.get("{}/schema/".format(self.host))
            except Exception as e:
                self._log_error("Could not load schema from server: {}".format(e))
                return False

        try:
            self.client.action(self.schema, ['tasks', 'create'], params={'data': tasks})
        except Exception as e:
            self._log_error("Could not post tasks to server: {}".format(e))
            return False

        self._log_success("Posted {} task events to server.".format(len(tasks)))
        return True


def task_history_writer(queue, host, token):
    # Ensure that we POST task events at least and at most once every `timeout` secs.
    timeout = 10

    # Maxmimum number of events we'll cache.
    # If we exceed this, we'll stop caching and will drop new events. This is preferable to
    # leaving memory consumption unbounded.
    max_cache_size = 10000

    conn = SawyerServerConnection(host, token)
    next_try = time.time() + timeout
    cache = []

    while True:
        try:
            try:
                event = queue.get(block=False, timeout=timeout)
                if len(cache) < max_cache_size:
                    cache.append(event)
            except Empty:
                pass

            now = time.time()
            if len(cache) and now > next_try:
                success = conn.post(cache)
                if success:
                    cache = []
                next_try = now + timeout
        except Exception as e:
            # Catch base Exception because we don't want process to error out and terminate
            logger.warning("Exception in track_history_writer: {}".format(e))


# Because we're dealing with a remote service, we need to handle these cases:
# 1. sawyer-server isn't up when tracker is initialized
# 2. sawyer-server goes down for a bit (few task events pile up)
# 3. sawyer-server goes down for a while (many task events pile up)

class SawyerTaskHistory(TaskHistory):
    def __init__(self, *args, **kwargs):
        host, token = self._connection_args()
        if not (host and token):
            self.enabled = False
            logger.warning(
                "Cannot find SAWYER_ENV and SAWYER_AUTH_TOKEN in env. Tracking is disabled.")
        else:
            self.enabled = True
            self.queue = multiprocessing.Queue()
            multiprocessing.Process(target=task_history_writer, args=(self.queue, host, token)).start()
            logger.info(
                "Initialized sawyer task history tracker. Will post task events to {}".format(host))

    def _connection_args(self):
        host = os.getenv('SAWYER_API_HOST').rstrip('/')
        env = os.getenv('SAWYER_ENV')
        token = os.getenv('SAWYER_AUTH_TOKEN')

        if env and not host:
            host = "https://sawyer-api.{}.lexent.bio".format(env)

        return host, token

    def _enqueue_event(self, task, status=None, deps=None):
        if not self.enabled:
            return

        status = status or task.status

        # TODO: remove before merging
        if not hasattr(task, 'run_id'):
            task.run_id = 1

        if hasattr(task, 'run_id'):
            self.queue.put({
                'modified_at': datetime.now().isoformat(),
                'task_id': task.id,
                'params': task.params or {},
                'status': status.lower() or 'pending',
                'deps': deps or [],
                'events': [],
                'run': task.run_id,
            })

    def task_scheduled(self, task):
        self._enqueue_event(task, status='pending')

    def task_finished(self, task, successful):
        self._enqueue_event(task, status='done' if successful else 'failed')

    def task_started(self, task, worker_host):
        self._enqueue_event(task)

    def task_new_deps(self, task, deps, newdeps):
        if deps:
            status = 'staticdeps'
        elif newdeps:
            status = 'dyndeps'
        else:
            status = 'unknowndeps'

        self._enqueue_event(task, status=status, deps=(deps or []) + (newdeps or []))
