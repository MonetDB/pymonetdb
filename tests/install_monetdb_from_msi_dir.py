#!/usr/bin/env python3

import os
import shutil
import sys

BASE = 'https://www.monetdb.org/downloads/Windows/Latest/'

if len(sys.argv) != 3:
    exit(f"Usage: {sys.argv[0]} <STAGING> <DEST>")
staging_dir = sys.argv[1]
dest_dir = sys.argv[2]

TREE = """
bin/
bin/bat.dll
bin/bat.pdb
bin/bz2.dll
bin/charset-1.dll
bin/geos.dll
bin/geos_c.dll
bin/getopt.dll
bin/iconv-2.dll
bin/libxml2.dll
bin/lz4.dll
bin/lzma.dll
bin/mapi.dll
bin/mapi.pdb
bin/mclient.exe
bin/mclient.pdb
bin/monetdb5.dll
bin/monetdb5.pdb
bin/monetdbe.dll
bin/monetdbsql.dll
bin/mserver5.exe
bin/mserver5.pdb
bin/msqldump.exe
bin/msqldump.pdb
bin/pcre.dll
bin/stream.dll
bin/stream.pdb
bin/zlib1.dll
etc/
etc/.monetdb
include/
include/monetdb/
include/monetdb/copybinary.h
include/monetdb/exception_buffer.h
include/monetdb/gdk.h
include/monetdb/gdk_atoms.h
include/monetdb/gdk_bbp.h
include/monetdb/gdk_calc.h
include/monetdb/gdk_cand.h
include/monetdb/gdk_delta.h
include/monetdb/gdk_hash.h
include/monetdb/gdk_posix.h
include/monetdb/gdk_strimps.h
include/monetdb/gdk_system.h
include/monetdb/gdk_time.h
include/monetdb/gdk_tracer.h
include/monetdb/gdk_utils.h
include/monetdb/mal.h
include/monetdb/mal_authorize.h
include/monetdb/mal_client.h
include/monetdb/mal_errors.h
include/monetdb/mal_exception.h
include/monetdb/mal_function.h
include/monetdb/mal_import.h
include/monetdb/mal_instruction.h
include/monetdb/mal_linker.h
include/monetdb/mal_listing.h
include/monetdb/mal_module.h
include/monetdb/mal_namespace.h
include/monetdb/mal_prelude.h
include/monetdb/mal_resolve.h
include/monetdb/mal_stack.h
include/monetdb/mal_type.h
include/monetdb/mapi.h
include/monetdb/matomic.h
include/monetdb/mel.h
include/monetdb/monet_getopt.h
include/monetdb/monet_options.h
include/monetdb/monetdb_config.h
include/monetdb/monetdbe.h
include/monetdb/mstring.h
include/monetdb/opt_backend.h
include/monetdb/rel_basetable.h
include/monetdb/rel_distribute.h
include/monetdb/rel_dump.h
include/monetdb/rel_exp.h
include/monetdb/rel_optimizer.h
include/monetdb/rel_partition.h
include/monetdb/rel_prop.h
include/monetdb/rel_rel.h
include/monetdb/rel_semantic.h
include/monetdb/sql_atom.h
include/monetdb/sql_backend.h
include/monetdb/sql_catalog.h
include/monetdb/sql_hash.h
include/monetdb/sql_import.h
include/monetdb/sql_keyword.h
include/monetdb/sql_list.h
include/monetdb/sql_mem.h
include/monetdb/sql_mvc.h
include/monetdb/sql_parser.h
include/monetdb/sql_privileges.h
include/monetdb/sql_qc.h
include/monetdb/sql_query.h
include/monetdb/sql_relation.h
include/monetdb/sql_scan.h
include/monetdb/sql_semantic.h
include/monetdb/sql_stack.h
include/monetdb/sql_storage.h
include/monetdb/sql_string.h
include/monetdb/sql_symbol.h
include/monetdb/sql_tokens.h
include/monetdb/sql_types.h
include/monetdb/store_sequence.h
include/monetdb/stream.h
include/monetdb/stream_socket.h
lib/
lib/bat.lib
lib/bz2.lib
lib/charset.lib
lib/getopt.lib
lib/iconv.lib
lib/libxml2.lib
lib/lz4.lib
lib/lzma.lib
lib/mapi.lib
lib/monetdb5.lib
lib/monetdb5/
lib/monetdb5/_generator.dll
lib/monetdb5/_generator.pdb
lib/monetdb5/_geom.dll
lib/monetdb5/_geom.pdb
lib/monetdb5/_pyapi3.dll
lib/monetdb5/_pyapi3.pdb
lib/monetdbe.lib
lib/monetdbsql.lib
lib/pcre.lib
lib/stream.lib
lib/zlib.lib
license.rtf
M5server.bat
mclient.bat
msqldump.bat
MSQLserver.bat
pyapi_locatepython3.bat
share/
share/doc/
share/doc/MonetDB-SQL/
share/doc/MonetDB-SQL/dump-restore.html
share/doc/MonetDB-SQL/dump-restore.txt
share/doc/MonetDB-SQL/website.html
System64/
System64/concrt140.dll
System64/msvcp140.dll
System64/msvcp140_1.dll
System64/msvcp140_2.dll
System64/msvcp140_atomic_wait.dll
System64/msvcp140_codecvt_ids.dll
System64/vccorlib140.dll
System64/vcruntime140.dll
System64/vcruntime140_1.dll
"""


failures = 0
for line in TREE.strip().splitlines():
    parts = line.split('/')
    if not parts[-1]:
        continue
    src0 = parts[-1]
    src0 = src0.replace('-', '_')
    if src0.startswith('.'):
        src0 = '_' + src0
    if '140' in src0 and '.dll' in src0:
        src0 += '.DFEFC2FE_EEE6_424C_841B_D4E66F0C84A3'
    src = os.path.join(staging_dir, src0)
    tgt_dir = os.path.join(dest_dir, *parts[:-1])
    tgt = os.path.join(dest_dir, *parts)
    if not os.path.isdir(tgt_dir):
        print(f"Creating dir {tgt_dir}", flush=True)
        os.makedirs(tgt_dir)
    print(f"Copying [{src0}] {src} to {tgt}", flush=True)
    try:
        shutil.copyfile(src, tgt)
    except Exception as e:
        print(f"  !! FAILED: {e}", flush=True)
        failures += 1

if failures:
    exit(f"Encountered {failures} failures")
