## Run

`./main --bind ':1234' --db 'host=/tmp dname=queue' --max-connection 40`

## Api

* /healthcheck
* /create request: `{"id": "text", "payload": "{}"}`
* /status request: `{"id": "text"}`

Responses:
```
{
    "ok": bool,
    "error_code": internal error code (int),
    "description": expand error message (text),
    "result": raw json (text)
}
```
