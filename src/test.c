/*
   Copyright (c) 2019 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include <stdio.h>
int main(int argc,char**argv)
{
	char c[] = "abc";
	printf("\nsize: %lu", sizeof(c));
	printf("\nsize: %lu", sizeof("def"));
}
