enable_language(ASM)
set(ASM_OPTIONS "-x assembler-with-cpp")
set(
  context_srcs
  "${BOOST_SOURCE}/libs/context/src/dummy.cpp"
  "${BOOST_SOURCE}/libs/context/src/posix/stack_traits.cpp"
)

if(SANITIZE AND (SANITIZE STREQUAL "address" OR SANITIZE STREQUAL "thread"))
  add_compile_definitions(BOOST_USE_UCONTEXT)

  if(SANITIZE STREQUAL "address")
    add_compile_definitions(BOOST_USE_ASAN)
  elseif(SANITIZE STREQUAL "thread")
    add_compile_definitions(BOOST_USE_TSAN)
  endif()

  set(
    context_srcs ${context_srcs}
    "${BOOST_SOURCE}/libs/context/src/fiber.cpp"
    "${BOOST_SOURCE}/libs/context/src/continuation.cpp"
  )
endif()

message("plantform(${CMAKE_SYSTEM_PROCESSOR}): ARCH_AARCH64 : ${ARCH_AARCH64}\t ARCH_PPC64LE : ${ARCH_PPC64LE} \t ARCH_RISCV64 : ${ARCH_RISCV64} \t OS_DARWIN : ${OS_DARWIN}")

if(ARCH_AARCH64)
  if(OS_DARWIN)
    set(
      context_srcs ${context_srcs}
      "${BOOST_SOURCE}/libs/context/src/asm/jump_arm64_aapcs_macho_gas.S"
      "${BOOST_SOURCE}/libs/context/src/asm/make_arm64_aapcs_macho_gas.S"
      "${BOOST_SOURCE}/libs/context/src/asm/ontop_arm64_aapcs_macho_gas.S"
    )
  else()
    set(
      context_srcs ${context_srcs}
      "${BOOST_SOURCE}/libs/context/src/asm/jump_arm64_aapcs_elf_gas.S"
      "${BOOST_SOURCE}/libs/context/src/asm/make_arm64_aapcs_elf_gas.S"
      "${BOOST_SOURCE}/libs/context/src/asm/ontop_arm64_aapcs_elf_gas.S"
    )
  endif()
elseif(ARCH_PPC64LE)
  set(
    context_srcs ${context_srcs}
    "${BOOST_SOURCE}/libs/context/src/asm/jump_ppc64_sysv_elf_gas.S"
    "${BOOST_SOURCE}/libs/context/src/asm/make_ppc64_sysv_elf_gas.S"
    "${BOOST_SOURCE}/libs/context/src/asm/ontop_ppc64_sysv_elf_gas.S"
  )
elseif(ARCH_RISCV64)
  set(
    context_srcs ${context_srcs}
    "${BOOST_SOURCE}/libs/context/src/asm/jump_riscv64_sysv_elf_gas.S"
    "${BOOST_SOURCE}/libs/context/src/asm/make_riscv64_sysv_elf_gas.S"
    "${BOOST_SOURCE}/libs/context/src/asm/ontop_riscv64_sysv_elf_gas.S"
  )
elseif(OS_DARWIN)
  set(
    context_srcs ${context_srcs}
    "${BOOST_SOURCE}/libs/context/src/asm/jump_x86_64_sysv_macho_gas.S"
    "${BOOST_SOURCE}/libs/context/src/asm/make_x86_64_sysv_macho_gas.S"
    "${BOOST_SOURCE}/libs/context/src/asm/ontop_x86_64_sysv_macho_gas.S"
  )
else()
  set(
    context_srcs ${context_srcs}
    "${BOOST_SOURCE}/libs/context/src/asm/jump_x86_64_sysv_elf_gas.S"
    "${BOOST_SOURCE}/libs/context/src/asm/make_x86_64_sysv_elf_gas.S"
    "${BOOST_SOURCE}/libs/context/src/asm/ontop_x86_64_sysv_elf_gas.S"
  )
endif()

_add_boost_lib(
  NAME context
  SOURCES
  ${context_srcs}
  DEFINE_PRIVATE
  BOOST_CONTEXT_SOURCE=1
  BOOST_CONTEXT_EXPORT
  LINK
  Boost::thread
)

set(Boost_CONTEXT_LIBRARIES "boost::context")

#_add_boost_test(
#  NAME context_test
#  LINK
#    Boost::context
#    Boost::unit_test_framework
#  TESTS
#    RUN ${BOOST_SOURCE}/libs/context/test/test_invoke.cpp
#    RUN ${BOOST_SOURCE}/libs/context/test/test_apply.cpp
#    RUN ${BOOST_SOURCE}/libs/context/test/test_fcontext.cpp
#    RUN ${BOOST_SOURCE}/libs/context/test/test_fiber.cpp
#    RUN ${BOOST_SOURCE}/libs/context/test/test_callcc.cpp
#    RUN ${BOOST_SOURCE}/libs/context/test/test_execution_context_v2.cpp
#)
