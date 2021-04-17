

class BaseTask(app.Task):  # app is the initialized celery instance:  app = celery.Celery(...)
    def __call__(self, *args, **kwargs):
        log.debug({
            "message": "Starting task",
            "args": self.request.args,
            "kwargs": self.request.kwargs,
            "retry": "{}/{}".format(self.request.retries, self.max_retries),
        })
        try:
            task_return_value = super().__call__(*args, **kwargs)
        except Exception as e:
            log.error({'message': 'Exception in task', 'exc': e})
            raise(e)
        else:
            if task_return_value == "I did the wrong stuff":
                # you can manually retry from wherever you need, in here, in the subtask, wherever
                self.retry()
            return task_return_value

    # these are just a few of the handlers you can override
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        log.critical({"message": "Task failed",
                      "req": self.request.__dict__,
                      "retries_attempted": "{}".format(self.request.retries),
                      "max_retries": "{}".format(self.max_retries),
                      "task_id": self.request.id,
                      "args": args,
                      "kwargs": kwargs,
                      "exc": exc,
                      "einfo": einfo})
        super().on_failure(exc, task_id, args, kwargs, einfo)

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        log.info({"message": "Task retrying",
                  "retries_attempted": "{}".format(self.request.retries),
                  "max_retries": "{}".format(self.max_retries),
                  "task_id": self.request.id,
                  "args": args,
                  "kwargs": kwargs,
                  "exc": exc,
                  "einfo": einfo})
        super().on_retry(exc, task_id, args, kwargs, einfo)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        log.info({"message": "Task returned",
                  "retries_attempted": "{}".format(self.request.retries),
                  "max_retries": "{}".format(self.max_retries),
                  "task_id": self.request.id,
                  "args": args,
                  "kwargs": kwargs,
                  "einfo": einfo})
        return retval



@app.task(
    base=BaseTask,
    bind=True,
    default_retry_delay=2,
    retry_backoff=True,
    rate_limit=10,
    acks_late=True,
    # can add specific errors here to make it more useful, i.e. only retry on retryable failures
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 5}
)
def maybe_do_stuff(self, *args, **kwargs):
    import random
    if random.randint(0,1):
        return "I did stuff"
    else:
        # with the current autoretry settings in the decorator, this will trigger a retry.
        raise Exception("I didn't do stuff")

