# Toy SQLite implementation in Rust

Following the [Let's Build a Simple Database][0] tutorial to understand the database internals.

Implemented till [part-13][1]. The code became highly unreadable while implementing [part-14][2] due to borrow checker.

My goal was to implement this in safe Rust and also without the use of `Rc<RefCell<T>>`. I reasoned that `Rc<RefCell<T>>` is just a fancy garbage collector, and I did not want this overhead for the implementation of B+Tree.

The code for [part-14][2] is in the branch [rust-sqlite/tree/part-14][3], but this has some bugs and I am not feeling like fixing it at the moment.

[0]: https://cstack.github.io/db_tutorial/
[1]: https://cstack.github.io/db_tutorial/parts/part13.html
[2]: https://cstack.github.io/db_tutorial/parts/part14.html
[3]: https://github.com/conscious-puppet/rust-sqlite/tree/part-14
