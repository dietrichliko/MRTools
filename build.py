import os
import subprocess
import pathlib
from typing import Any, Dict

from distutils.file_util import copy_file
from setuptools.command.build_ext import build_ext
from setuptools.extension import Extension


class CMakeROOTDictExtension(Extension):
    name: str

    def __init__(self, name: str, sourcedir: str = "") -> None:
        super().__init__(name, sources=[])
        self.sourcedir = str(pathlib.Path(sourcedir).resolve())


class ExtensionBuilder(build_ext):
    def run(self) -> None:
        self.validate_cmake()
        super().run()

    def build_extension(self, ext: Extension) -> None:
        if isinstance(ext, CMakeROOTDictExtension):
            self.build_cmake_extension(ext)
        else:
            super().build_extension(ext)

    def validate_cmake(self) -> None:
        cmake_extensions = [
            x for x in self.extensions if isinstance(x, CMakeROOTDictExtension)
        ]
        if len(cmake_extensions) > 0:
            try:
                subprocess.check_call(["cmake", "--version"])
            except OSError:
                raise RuntimeError(
                    "CMake must be installed to build the following extensions: "
                    + ", ".join(e.name for e in cmake_extensions)
                )

    def build_cmake_extension(self, ext: CMakeROOTDictExtension) -> None:
        extdir = pathlib.Path(self.get_ext_fullpath(ext.name)).parent.resolve()
        cmake_args = ["-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=" + str(extdir)]

        cfg = "Debug" if self.debug else "Release"
        # cfg = 'Debug'
        build_args = ["--config", cfg]

        cmake_args += ["-DCMAKE_BUILD_TYPE=" + cfg]
        build_args += ["--", "-j4"]

        env = os.environ.copy()
        env["CXXFLAGS"] = '{} -DVERSION_INFO=\\"{}\\"'.format(
            env.get("CXXFLAGS", ""), self.distribution.get_version()
        )
        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)
        subprocess.check_call(
            ["cmake", ext.sourcedir] + cmake_args, cwd=self.build_temp, env=env
        )
        subprocess.check_call(
            ["cmake", "--build", "."] + build_args, cwd=self.build_temp
        )

    def copy_extensions_to_source(self):
        build_py = self.get_finalized_command("build_py")
        for ext in self.extensions:
            fullname = self.get_ext_fullname(ext.name)
            modpath = fullname.split(".")
            package = ".".join(modpath[:-1])
            if isinstance(ext, CMakeROOTDictExtension):
                filename = os.path.join(*modpath[:-1], f"lib{modpath[-1]}.so")
            else:
                fullname = self.get_ext_fullname(ext.name)
                filename = self.get_ext_filename(fullname)
            package_dir = build_py.get_package_dir(package)

            # Always copy, even if source is older than destination, to ensure
            # that the right extensions for the current Python/platform are
            # used.
            copy_file(
                os.path.join(self.build_lib, filename),
                os.path.join(package_dir, os.path.basename(filename)),
                verbose=self.verbose,
                dry_run=self.dry_run,
            )
            if ext._needs_stub:
                self.write_stub(package_dir or os.curdir, ext, True)

            # collect also the ROOT Dictionaty artifacst
            if isinstance(ext, CMakeROOTDictExtension):
                for name in os.listdir(os.path.join(self.build_lib, *modpath[:-1])):
                    if name.split(".")[-1] in ["pcm", "rootmap"]:
                        copy_file(
                            os.path.join(self.build_lib, *modpath[:-1], name),
                            os.path.join(package_dir, name),
                            verbose=self.verbose,
                            dry_run=self.dry_run,
                        )


def build(setup_kwargs: Dict[str, Any]) -> None:

    ext_modules = [CMakeROOTDictExtension("mrtools.MRTools", sourcedir="src/mrtools/cxx")]
    setup_kwargs.update(
        {
            "ext_modules": ext_modules,
            "cmdclass": dict(build_ext=ExtensionBuilder),
            "zip_safe": False,
        }
    )
