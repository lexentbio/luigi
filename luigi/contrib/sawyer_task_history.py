from datetime import datetime
import logging
import multiprocessing
from queue import Empty
import time

import coreapi
from coreapi import codecs
import openapi_codec

from luigi.task_history import TaskHistory


logger = logging.getLogger(__name__)


def task_history_writer(queue, poster):
    last_post = time.time()
    cached_events = []

    # Ensure that we send messages at least and at most once every 10 secs.
    # When scheduler is idle, block on queue for 10 secs, check cache and repeat.
    # When scheduler is busy, collect messages in cache for 10 secs, then send together.
    timeout = 10

    while True:
        try:
            try:
                event = queue.get(block=False, timeout=timeout)
                cached_events.append(event)
            except Empty:
                pass

            now = time.time()
            if len(cached_events) and now - last_post > timeout:
                for event in cached_events:
                    logger.debug(event)

                poster(cached_events)
                cached_events = []
                last_post = now
        except Exception as e:
            # Catch base Exception because we don't want process to error out and terminate
            logger.warning("Exception in track_history_writer: {}".format(e))


class SawyerTaskHistory(TaskHistory):
    def __init__(self, *args, **kwargs):
        # TODO: read from config or env
        self.api_server_uri = 'http://127.0.0.1:8000'

        logger.info("Initializing sawyer task history tracker. Will post to {}.".format(self.api_server_uri))

        self.queue = multiprocessing.Queue()
        poster = self._api_poster()
        multiprocessing.Process(target=task_history_writer, args=(self.queue, poster)).start()

    def _api_poster(self):
        client = coreapi.Client(
            decoders=[openapi_codec.OpenAPICodec(), codecs.JSONCodec()])
        schema = client.get("{}/schema/".format(self.api_server_uri))

        def poster(tasks):
            logger.info("Posting {} task events to sawyer-api.".format(len(tasks)))
            client.action(schema, ['tasks', 'create'], params={'data': tasks})

        return poster

    def _enqueue_event(self, task, status=None, deps=None):
        status = status or task.status

        # For testing only
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
        self._enqueue_event(task, status='newdeps', deps=(deps or []) + (newdeps or []))
