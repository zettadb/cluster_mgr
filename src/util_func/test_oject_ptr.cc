#include "object_ptr.h"
#include "../kl_mentain/thread_pool.h"
#include <iostream>
#include<unistd.h>

using namespace kunlun;
using namespace kunlun_rbr;

class A : public ObjectRef {
public:
    A() : a(0) {}
    virtual ~A() {}

    void print() {
        std::cout << "a: " << a << std::endl;
    }
private:
    int a;
};

int test_func(ObjectPtr<A> ptr) {
    std::cout << "func A ref: " << ptr.GetTRef() << std::endl;
    ptr->print();
    return 0;
}

int main(int argc, char** argv) {
    ObjectPtr<A> ptrA( new A());
    std::cout << "ptrA is valid: " << ptrA.Invalid() << std::endl;
    {
        ObjectPtr<A> ptrB = ptrA;
        std::cout << "A ref: " << ptrA.GetTRef() << std::endl;
    }

    A* a = ptrA.GetTRaw();
    a->print();

    {
        ObjectPtr<A> ptrC;
        std::cout << "ptrC is valid: " << ptrC.Invalid() << std::endl;
        ObjectPtr<A> ptrD(ptrA);
        std::cout << "A ref: " << ptrA.GetTRef() << std::endl;
    }

    CThreadPool tpool(1);
    tpool.commit([ptrA]{
        sleep(10);
        std::cout << "pool A ref: " << ptrA.GetTRef() << std::endl;
    });


    std::cout << "1.A ref: " << ptrA.GetTRef() << std::endl;
    ptrA->print();

    sleep(20);
    test_func(ptrA);

    std::vector<ObjectPtr<A> > A_vec;
    A_vec.push_back(ptrA);
    for(auto it : A_vec) {
        std::cout << "vec A ref: " << it.GetTRef() << std::endl;
    }

    std::cout << "2.A ref: " << ptrA.GetTRef() << std::endl;
    return 0;
}