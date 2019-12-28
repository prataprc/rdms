if [ $? -eq 0 ] ; then
    echo "cargo test ...................."
    cargo test
fi

#if [ $? -eq 0 ] ; then
#    echo "cargo test -- --ignored .................."
#    cargo test -- --ignored
#fi

if [ $? -eq 0 ] ; then
    echo "cargo test --release ....................."
    cargo test --release
fi

if [ $? -eq 0 ] ; then
    echo "cargo test --release -- --ignored .................."
    cargo test --release -- --ignored
fi
