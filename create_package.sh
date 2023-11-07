echo "删除历史文件"
rmdir="rm -r ./build"
eval $rmdir
echo "$rmdir"
rmdir2="rm -r ./dist"
eval $rmdir2
echo "$rmdir2"
rmdir3="rm -r ./py_sdk.egg-info"
eval $rmdir3
echo "$rmdir3"

echo "开始打包"
build="python setup.py sdist bdist_wheel"
eval $build
echo "$build"


echo "检查打包文件"
check="twine check dist/*"
eval $check
echo "$check"

upload="twine upload dist/* --repository-url"
eval $upload
echo "$upload"

echo "上传完毕"