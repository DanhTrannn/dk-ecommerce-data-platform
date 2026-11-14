import hashlib
import hmac
import json
import stat
import time
from pathlib import Path

import jwt


def canonical_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def make_token(key, actor, method, path, payload=None):
    return jwt.encode({'sub': str(actor), 'aud': 'credential-exporter', 'exp': int(time.time()) + 60,
                       'method': method, 'path': path, 'digest': canonical_digest(payload)}, key, algorithm='HS256')


def read_key(path):
    path = Path(path)
    if path.is_symlink() or not stat.S_ISREG(path.stat().st_mode) or stat.S_IMODE(path.stat().st_mode) != 0o600:
        raise ValueError('Invalid signing key')
    key = path.read_bytes()
    if len(key) < 32:
        raise ValueError('Invalid signing key')
    return key


def authenticate(key, token, method, path, payload=None):
    claims = jwt.decode(token, key, algorithms=['HS256'], audience='credential-exporter',
                        options={'require': ['sub', 'aud', 'exp', 'method', 'path', 'digest']})
    if (not isinstance(claims['sub'], str) or not claims['sub'] or claims['exp'] > time.time() + 60
            or claims['method'] != method or claims['path'] != path
            or not isinstance(claims['digest'], str)
            or not hmac.compare_digest(claims['digest'], canonical_digest(payload))):
        raise ValueError('Invalid authorization')
    return claims['sub']
