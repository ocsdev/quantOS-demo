project(jzts_api)

set(SRC
    __init__.py
    jrpc.py
    jrpc_py.py
    utils.py
    data_api.py
)

install (FILES
    __init__.py
    jrpc.py
    jrpc_py.py
    utils.py
    data_api.py
    DESTINATION ${LIB_PATH}/jzquant
)