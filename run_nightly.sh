if [ $? -eq 0 ] ; then
    echo "cargo test ...................."
    cargo test
fi

#if [ $? -eq 0 ] ; then
#    echo "cargo test -- --ignored .................."
#    cargo test -- --ignored
#fi

if [ $? -ne 0 ] ; then
    exit 1
fi

echo "cargo test --release ....................."
cargo test --release

if [ $? -ne 0 ] ; then
    exit 1
fi

echo "cargo test --release -- --ignored .................."
cargo test --release -- --ignored
