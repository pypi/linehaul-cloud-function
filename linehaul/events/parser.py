# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import enum
import logging
import posixpath
import re

from datetime import datetime, timezone
from typing import Optional

import attr
import attr.validators
import cattr

from linehaul.ua import UserAgent, parser as user_agents


logger = logging.getLogger(__name__)


# detailed_validation=False makes cattrs "fail fast": the first validator error
# during structuring propagates as-is (e.g. a plain TypeError/ValueError) instead
# of being wrapped in a cattrs ClassValidationError (an ExceptionGroup, the
# library default since cattrs 22.1). We rely on this because a malformed line
# should just raise so the caller can shunt it to the unprocessed/ bucket -- the
# exception type is never surfaced to a human, so the richer aggregated error is
# pure overhead here, and the parser tests assert the bare TypeError/ValueError.
_cattr = cattr.Converter(detailed_validation=False)
_cattr.register_structure_hook(
    datetime,
    lambda d, t: datetime.strptime(d[5:-4], "%d %b %Y %H:%M:%S").replace(
        tzinfo=timezone.utc
    ),
)


class UnparseableEvent(Exception):
    pass


# Slightly looser than pyparsing's default whitespace (" \t\n"): "\r" is skipped
# around delimiters too, so a stray carriage return between fields no longer
# rejects the line.
_WS = r"[ \t\n\r]*+"
_WORD = r"[!-?A-{}~][ \t!-?A-{}~]*+"


def _nullable(name):
    return rf"(?:\(null\)|(?P<{name}>(?!\(null\)){_WORD}))"


_PACKAGE_TYPE = (
    r"(?:\(null\)|(?P<package_type>sdist|bdist_wheel|bdist_dmg|bdist_dumb"
    r"|bdist_egg|bdist_msi|bdist_rpm|bdist_wininst))"
)
_TAIL = r"(?P<user_agent>[^\n]*+)(?:\n[ \t\n\r]*+)?\Z"
_COMMON = (
    rf"{_WS}\|{_WS}(?P<timestamp>{_WORD}){_WS}\|"
    rf"(?:{_WS}(?P<country_code>{_WORD}))?{_WS}\|"
    rf"{_WS}(?P<url>{_WORD}){_WS}\|"
    rf"{_WS}{_nullable('tls_protocol')}{_WS}\|"
    rf"{_WS}{_nullable('tls_cipher')}{_WS}\|"
)

# Two separate patterns rather than one alternation: duplicate group names across
# branches are a syntax error before Python 3.12, and we target 3.11.
MESSAGE_v3 = re.compile(
    rf"\A{_WS}download{_COMMON}{_WS}{_nullable('project_name')}{_WS}\|"
    rf"{_WS}{_nullable('version')}{_WS}\|{_WS}{_PACKAGE_TYPE}{_WS}\|{_TAIL}"
)
MESSAGE_SIMPLE = re.compile(rf"\A{_WS}simple{_COMMON}{_WS}\|{_WS}\|{_WS}\|{_TAIL}")


@enum.unique
class PackageType(enum.Enum):
    bdist_dmg = "bdist_dmg"
    bdist_dumb = "bdist_dumb"
    bdist_egg = "bdist_egg"
    bdist_msi = "bdist_msi"
    bdist_rpm = "bdist_rpm"
    bdist_wheel = "bdist_wheel"
    bdist_wininst = "bdist_wininst"
    sdist = "sdist"


@attr.s(slots=True, frozen=True)
class File:
    filename = attr.ib(validator=attr.validators.instance_of(str))
    project = attr.ib(validator=attr.validators.instance_of(str))
    version = attr.ib(validator=attr.validators.instance_of(str))
    type = attr.ib(type=PackageType)


@attr.s(slots=True, frozen=True)
class Download:
    timestamp = attr.ib(type=datetime)
    url = attr.ib(validator=attr.validators.instance_of(str))
    project = attr.ib(validator=attr.validators.instance_of(str))
    file = attr.ib(type=File)
    tls_protocol = attr.ib(
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)),
    )
    tls_cipher = attr.ib(
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)),
    )
    country_code = attr.ib(
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)),
    )
    details = attr.ib(type=Optional[UserAgent], default=None)


@attr.s(slots=True, frozen=True)
class Simple:
    timestamp = attr.ib(type=datetime)
    url = attr.ib(validator=attr.validators.instance_of(str))
    project = attr.ib(validator=attr.validators.instance_of(str))
    tls_protocol = attr.ib(
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)),
    )
    tls_cipher = attr.ib(
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)),
    )
    country_code = attr.ib(
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)),
    )
    details = attr.ib(type=Optional[UserAgent], default=None)


def parse(message):
    # parse_string() used to expandtabs() the input before parsing, so every
    # captured value -- including the user agent -- had its tabs expanded. Keep
    # doing it, or stored values would silently change.
    expanded = message.expandtabs()

    simple = True
    parsed = MESSAGE_SIMPLE.match(expanded)
    if parsed is None:
        simple = False
        parsed = MESSAGE_v3.match(expanded)
        if parsed is None:
            raise UnparseableEvent("{!r} does not match a known event".format(message))

    parsed = parsed.groupdict()

    url = parsed["url"]

    data = {}
    data["timestamp"] = parsed["timestamp"]
    data["tls_protocol"] = parsed["tls_protocol"]
    data["tls_cipher"] = parsed["tls_cipher"]
    data["country_code"] = parsed["country_code"]
    data["url"] = url
    data["file"] = {}
    data["file"]["filename"] = posixpath.basename(url)
    data["file"]["project"] = parsed.get("project_name")
    data["file"]["version"] = parsed.get("version")
    data["file"]["type"] = parsed.get("package_type")

    if simple:
        data["project"] = url.split("/")[2]
        result = _cattr.structure(data, Simple)
    else:
        data["project"] = parsed["project_name"]
        result = _cattr.structure(data, Download)

    try:
        ua = user_agents.parse(parsed["user_agent"])
        if ua is None:
            return  # Ignored user agents mean we'll skip trying to log this event
    except user_agents.UnknownUserAgentError:
        pass
    else:
        result = attr.evolve(result, details=ua)

    return result
