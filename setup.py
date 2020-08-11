
# Use this command for deploy.
#   python3 setup.py sdist bdist_wheel && python3 -m twine upload --skip-existing dist/*

import io
from setuptools import find_packages, setup
from glob import glob


setup(name='easy_cloudrun',
      version='1.0.1',
      description='GCP Cloud Run 을 사용 할 때 테스트, 빌드, 배포, GCP 이미지 삭제 등을 간단하게 해주는 패키지입니다.',
      long_description="Please refer to the https://github.com/da-huin/easy_cloudrun",
      long_description_content_type="text/markdown",
      url='https://github.com/da-huin/easy_cloudrun',
      download_url='https://github.com/da-huin/easy_cloudrun/archive/master.zip',
      author='JunYeong Park',
      author_email='dahuin000@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=["pyyaml"],
      classifiers=[
          'Programming Language :: Python :: 3',
      ],
      include_package_data=True)
