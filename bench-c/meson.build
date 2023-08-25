project('mpi', 'c',
  version : '0.1',
  default_options : ['warning_level=3'])

mpi = dependency('mpi', version : '>=4')

exe = executable('mpi', 'mpi.c',
  install : true,
  dependencies : [mpi],
)

test('basic', exe)
