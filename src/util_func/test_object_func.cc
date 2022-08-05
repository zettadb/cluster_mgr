#include "object_ptr.h"
#include <iostream>
#include<unistd.h>

using namespace kunlun;

class B : public ObjectRef {
public:
    B() {}
    virtual ~B() {}

    void deal() {
        std::cout << "B deal" << std::endl;
        if(call_back_) {
            call_back_(cb_context_);
        }
        std::cout << "B ref: " << this->GetRef() << std::endl;
    }
    void Set_call_back(void (*function)(void *)) {
        call_back_ = function;
    }
    void Set_cb_context(void *context) {
        cb_context_ = context;
    }

    void (*call_back_)(void *);
    void *cb_context_;
};

class A : public ObjectRef {
public:
    A() : a(0) {}
    virtual ~A() {}
    void Set() {
        a += 1;
    }
    void Set(int i) {
        a = i;
    }

    void deal() {
        ObjectPtr<B> ptrB(new B());
        ptrB->Set_call_back(A::callcb);
        this->IncRef();
        ptrB->Set_cb_context(this);
        ptrB->deal();
    }

    void print() {
        std::cout << "a: " << a << std::endl;
    }

public:
    static void callcb(void* arg);
private:
    int a;
};

void A::callcb(void* arg) {
    //A* ptrA = static_cast<A*>(arg);
    ObjectPtr<A> ptrA(static_cast<A*>(arg));
    ptrA->Set();
    ptrA->print();
    std::cout << "callcb A ref: " << ptrA->GetRef() << std::endl;
}

void func_3(void* ptr) {
    std::cout << "enter func_3" << std::endl;
    ObjectPtr<A> ptrA(static_cast<A*>(ptr));
    ptrA->Set();
    ptrA->print();
    std::cout << "func_3 A ref: " << ptrA.GetTRef() << std::endl;
    std::cout << "leave func_3" << std::endl;
    return ;
}

void func_2(ObjectPtr<A> ptrA) {
    std::cout << "enter func_2" << std::endl;
    std::cout << "func_2 1 A ref: " << ptrA.GetTRef() << std::endl;
    func_3(ptrA.GetTRaw());
    std::cout << "func_2 2 A ref: " << ptrA.GetTRef() << std::endl;
    std::cout << "leave func_2" << std::endl;
}  

void func_1() {
    std::cout << "enter func_1" << std::endl;
    ObjectPtr<A> ptrA(new A);
    std::cout << "func_1 1 A ref: " << ptrA.GetTRef() << std::endl;
    ptrA->print();
    func_2(ptrA);
    std::cout << "func_1 2 A ref: " << ptrA.GetTRef() << std::endl;
    std::cout << "leave func_1" << std::endl;
    return;
}

std::vector<ObjectPtr<A> > func_vector() {
    std::vector<ObjectPtr<A> > vec_A;
    for(int i=0; i<5; i++) {
        ObjectPtr<A> ptrA(new A);
        ptrA->Set(i);
        vec_A.emplace_back(ptrA);
    }
    return vec_A;
}

ObjectPtr<A> func_4() {
    ObjectPtr<A> ptrA(new A());
    ptrA->print();
    std::cout << "func_4 A ref: " << ptrA.GetTRef() << std::endl;

    ObjectPtr<A> ptrB(ptrA.GetTRaw());
    return ptrB;
}

int main(int argc, char** argv) {
    //ObjectPtr<A> ptrA(func_1().GetTRaw());
    //func_1();

    //ObjectPtr<A> ptrA = func_4();
    
    ObjectPtr<A> ptrA(new A());
    ptrA->deal();
    std::cout << "main A ref: " << ptrA.GetTRef() << std::endl;
    
    auto vec_A = func_vector();
    for(auto va : vec_A) {
        va->print();
        std::cout << "main vector A ref: " << va.GetTRef() << std::endl;
    }

/*
    ObjectPtr<A> ptrB = func_2(ptrA);
    std::cout << "main 2 A ref: " << ptrA.GetTRef() << std::endl;
    std::cout << "main 1 B ref: " << ptrB.GetTRef() << std::endl;

    ObjectPtr<A> ptrC(func_2(ptrA).GetTRaw());
    std::cout << "main 3 A ref: " << ptrA.GetTRef() << std::endl;
    std::cout << "main 2 B ref: " << ptrB.GetTRef() << std::endl;
    std::cout << "main 1 C ref: " << ptrC.GetTRef() << std::endl;

    ObjectPtr<A> ptrC(func_2(ptrA).GetTRaw());
    std::cout << "main 2 A ref: " << ptrA.GetTRef() << std::endl;
    std::cout << "main 1 C ref: " << ptrC.GetTRef() << std::endl;*/
    std::cout << "main exit" << std::endl;
    return 0;
}