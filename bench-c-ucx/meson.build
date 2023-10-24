project('mpi', 'c',
  version : '0.1',
  default_options : ['warning_level=3'])

mpi = dependency('mpi', version : '>=4')
ucx = dependency('ucx', version : '>=1.14.0')
threads = dependency('threads')

exe = executable('mpi', 'mpi.c',
  install : true,
  dependencies : [mpi, ucx, threads],
)

test('basic', exe)
