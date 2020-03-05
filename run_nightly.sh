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
RUSTFLAGS=-g cargo test --release

if [ $? -ne 0 ] ; then
    exit 1
fi

echo "cargo test --release -- --ignored .................."
RUSTFLAGS=-g cargo test --release -- --ignored

if [ $? -ne 0 ] ; then
    exit 1
fi

echo "cargo test --release shrobt_commit_compact .................."
for i in 0 0 0 0 0 0 0 0 0; do
    RUSTFLAGS=-g cargo test --release shrobt_commit_compact;
    if [ $? -ne 0 ] ; then
        exit 1
    fi
done
