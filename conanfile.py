import json
from pathlib import Path

from conan import ConanFile
from conan.tools.layout import basic_layout
from conan.internal import check_duplicated_generator
from conan.tools.files import save


CONAN_COMMANDLINE_FILENAME = "conandeps.env"

class CommandlineDeps:
    def __init__(self, conanfile):
        """
        :param conanfile: ``< ConanFile object >`` The current recipe object. Always use ``self``.
        """
        self._conanfile = conanfile

    def generate(self) -> None:
        """
        Collects all dependencies and components, then, generating a Makefile
        """
        check_duplicated_generator(self, self._conanfile)

        host_req = self._conanfile.dependencies.host
        build_req = self._conanfile.dependencies.build  # tool_requires
        test_req = self._conanfile.dependencies.test

        include_dirs = []
        library_dirs = []

        # Filter the build_requires not activated for any requirement
        dependencies = [tup for tup in list(host_req.items()) + list(build_req.items()) + list(test_req.items()) if not tup[0].build]

        for require, dep in dependencies:
            # Require is not used at the moment, but its information could be used, and will be used in Conan 2.0
            if require.build:
                continue
            include_dir = Path(dep.package_folder) / 'include'
            package_dir = Path(dep.package_folder) / 'lib'
            include_dirs.append(str(include_dir))
            library_dirs.append(str(package_dir))

        content = json.dumps(dict(include_dirs=include_dirs, library_dirs=library_dirs))
        save(self._conanfile, CONAN_COMMANDLINE_FILENAME, content)
        self._conanfile.output.info(f"Generated {CONAN_COMMANDLINE_FILENAME}")


class python_driverConan(ConanFile):
    win_bash = False

    settings = "os", "compiler", "build_type", "arch"
    requires = "libev/4.33", "lz4/1.10.0"

    def layout(self):
        basic_layout(self)

    def generate(self):
        pc = CommandlineDeps(self)
        pc.generate()
