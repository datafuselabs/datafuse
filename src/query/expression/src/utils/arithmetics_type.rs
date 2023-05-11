// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This code is generated by src/query/codegen/src/writes/arithmetics_type.rs. DO NOT EDIT.

use crate::types::number::Number;
use crate::types::number::F32;
use crate::types::number::F64;

pub trait ResultTypeOfBinary: Sized {
    type AddMul: Number;
    type Minus: Number;
    type IntDiv: Number;
    type Modulo: Number;
    type LeastSuper: Number;
}

pub trait ResultTypeOfUnary: Sized {
    type Negate: Number;
    type Sum: Number;

    fn checked_add(self, _rhs: Self) -> Option<Self>;

    fn checked_sub(self, _rhs: Self) -> Option<Self>;

    fn checked_mul(self, _rhs: Self) -> Option<Self>;

    fn checked_div(self, _rhs: Self) -> Option<Self>;

    fn checked_rem(self, _rhs: Self) -> Option<Self>;
}

impl ResultTypeOfBinary for (u8, u8) {
    type AddMul = u16;
    type Minus = i16;
    type IntDiv = u8;
    type Modulo = u8;
    type LeastSuper = u8;
}

impl ResultTypeOfBinary for (u8, u16) {
    type AddMul = u32;
    type Minus = i32;
    type IntDiv = u16;
    type Modulo = u16;
    type LeastSuper = u16;
}

impl ResultTypeOfBinary for (u8, u32) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u32;
    type Modulo = u32;
    type LeastSuper = u32;
}

impl ResultTypeOfBinary for (u8, u64) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u64;
    type Modulo = u64;
    type LeastSuper = u64;
}

impl ResultTypeOfBinary for (u8, i8) {
    type AddMul = i16;
    type Minus = i16;
    type IntDiv = i8;
    type Modulo = u8;
    type LeastSuper = i8;
}

impl ResultTypeOfBinary for (u8, i16) {
    type AddMul = i32;
    type Minus = i32;
    type IntDiv = i16;
    type Modulo = u16;
    type LeastSuper = i16;
}

impl ResultTypeOfBinary for (u8, i32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = u32;
    type LeastSuper = i32;
}

impl ResultTypeOfBinary for (u8, i64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = u64;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (u8, F32) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i32;
    type Modulo = F64;
    type LeastSuper = F32;
}

impl ResultTypeOfBinary for (u8, F64) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (u16, u8) {
    type AddMul = u32;
    type Minus = i32;
    type IntDiv = u16;
    type Modulo = u8;
    type LeastSuper = u16;
}

impl ResultTypeOfBinary for (u16, u16) {
    type AddMul = u32;
    type Minus = i32;
    type IntDiv = u16;
    type Modulo = u16;
    type LeastSuper = u16;
}

impl ResultTypeOfBinary for (u16, u32) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u32;
    type Modulo = u32;
    type LeastSuper = u32;
}

impl ResultTypeOfBinary for (u16, u64) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u64;
    type Modulo = u64;
    type LeastSuper = u64;
}

impl ResultTypeOfBinary for (u16, i8) {
    type AddMul = i32;
    type Minus = i32;
    type IntDiv = i16;
    type Modulo = u8;
    type LeastSuper = i16;
}

impl ResultTypeOfBinary for (u16, i16) {
    type AddMul = i32;
    type Minus = i32;
    type IntDiv = i16;
    type Modulo = u16;
    type LeastSuper = i16;
}

impl ResultTypeOfBinary for (u16, i32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = u32;
    type LeastSuper = i32;
}

impl ResultTypeOfBinary for (u16, i64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = u64;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (u16, F32) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i32;
    type Modulo = F64;
    type LeastSuper = F32;
}

impl ResultTypeOfBinary for (u16, F64) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (u32, u8) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u32;
    type Modulo = u8;
    type LeastSuper = u32;
}

impl ResultTypeOfBinary for (u32, u16) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u32;
    type Modulo = u16;
    type LeastSuper = u32;
}

impl ResultTypeOfBinary for (u32, u32) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u32;
    type Modulo = u32;
    type LeastSuper = u32;
}

impl ResultTypeOfBinary for (u32, u64) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u64;
    type Modulo = u64;
    type LeastSuper = u64;
}

impl ResultTypeOfBinary for (u32, i8) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = u8;
    type LeastSuper = i32;
}

impl ResultTypeOfBinary for (u32, i16) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = u16;
    type LeastSuper = i32;
}

impl ResultTypeOfBinary for (u32, i32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = u32;
    type LeastSuper = i32;
}

impl ResultTypeOfBinary for (u32, i64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = u64;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (u32, F32) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i32;
    type Modulo = F64;
    type LeastSuper = F32;
}

impl ResultTypeOfBinary for (u32, F64) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (u64, u8) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u64;
    type Modulo = u8;
    type LeastSuper = u64;
}

impl ResultTypeOfBinary for (u64, u16) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u64;
    type Modulo = u16;
    type LeastSuper = u64;
}

impl ResultTypeOfBinary for (u64, u32) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u64;
    type Modulo = u32;
    type LeastSuper = u64;
}

impl ResultTypeOfBinary for (u64, u64) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u64;
    type Modulo = u64;
    type LeastSuper = u64;
}

impl ResultTypeOfBinary for (u64, i8) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = u8;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (u64, i16) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = u16;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (u64, i32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = u32;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (u64, i64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = u64;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (u64, F32) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (u64, F64) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (i8, u8) {
    type AddMul = i16;
    type Minus = i16;
    type IntDiv = i8;
    type Modulo = i16;
    type LeastSuper = i8;
}

impl ResultTypeOfBinary for (i8, u16) {
    type AddMul = i32;
    type Minus = i32;
    type IntDiv = i16;
    type Modulo = i32;
    type LeastSuper = i16;
}

impl ResultTypeOfBinary for (i8, u32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i64;
    type LeastSuper = i32;
}

impl ResultTypeOfBinary for (i8, u64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (i8, i8) {
    type AddMul = i16;
    type Minus = i16;
    type IntDiv = i8;
    type Modulo = i16;
    type LeastSuper = i8;
}

impl ResultTypeOfBinary for (i8, i16) {
    type AddMul = i32;
    type Minus = i32;
    type IntDiv = i16;
    type Modulo = i32;
    type LeastSuper = i16;
}

impl ResultTypeOfBinary for (i8, i32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i64;
    type LeastSuper = i32;
}

impl ResultTypeOfBinary for (i8, i64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (i8, F32) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i32;
    type Modulo = F64;
    type LeastSuper = F32;
}

impl ResultTypeOfBinary for (i8, F64) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (i16, u8) {
    type AddMul = i32;
    type Minus = i32;
    type IntDiv = i16;
    type Modulo = i16;
    type LeastSuper = i16;
}

impl ResultTypeOfBinary for (i16, u16) {
    type AddMul = i32;
    type Minus = i32;
    type IntDiv = i16;
    type Modulo = i32;
    type LeastSuper = i16;
}

impl ResultTypeOfBinary for (i16, u32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i64;
    type LeastSuper = i32;
}

impl ResultTypeOfBinary for (i16, u64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (i16, i8) {
    type AddMul = i32;
    type Minus = i32;
    type IntDiv = i16;
    type Modulo = i16;
    type LeastSuper = i16;
}

impl ResultTypeOfBinary for (i16, i16) {
    type AddMul = i32;
    type Minus = i32;
    type IntDiv = i16;
    type Modulo = i32;
    type LeastSuper = i16;
}

impl ResultTypeOfBinary for (i16, i32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i64;
    type LeastSuper = i32;
}

impl ResultTypeOfBinary for (i16, i64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (i16, F32) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i32;
    type Modulo = F64;
    type LeastSuper = F32;
}

impl ResultTypeOfBinary for (i16, F64) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (i32, u8) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i16;
    type LeastSuper = i32;
}

impl ResultTypeOfBinary for (i32, u16) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i32;
    type LeastSuper = i32;
}

impl ResultTypeOfBinary for (i32, u32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i64;
    type LeastSuper = i32;
}

impl ResultTypeOfBinary for (i32, u64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (i32, i8) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i16;
    type LeastSuper = i32;
}

impl ResultTypeOfBinary for (i32, i16) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i32;
    type LeastSuper = i32;
}

impl ResultTypeOfBinary for (i32, i32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i64;
    type LeastSuper = i32;
}

impl ResultTypeOfBinary for (i32, i64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (i32, F32) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i32;
    type Modulo = F64;
    type LeastSuper = F32;
}

impl ResultTypeOfBinary for (i32, F64) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (i64, u8) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i16;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (i64, u16) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i32;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (i64, u32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (i64, u64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (i64, i8) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i16;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (i64, i16) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i32;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (i64, i32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (i64, i64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
    type LeastSuper = i64;
}

impl ResultTypeOfBinary for (i64, F32) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (i64, F64) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (F32, u8) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i32;
    type Modulo = F64;
    type LeastSuper = F32;
}

impl ResultTypeOfBinary for (F32, u16) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i32;
    type Modulo = F64;
    type LeastSuper = F32;
}

impl ResultTypeOfBinary for (F32, u32) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i32;
    type Modulo = F64;
    type LeastSuper = F32;
}

impl ResultTypeOfBinary for (F32, u64) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (F32, i8) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i32;
    type Modulo = F64;
    type LeastSuper = F32;
}

impl ResultTypeOfBinary for (F32, i16) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i32;
    type Modulo = F64;
    type LeastSuper = F32;
}

impl ResultTypeOfBinary for (F32, i32) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i32;
    type Modulo = F64;
    type LeastSuper = F32;
}

impl ResultTypeOfBinary for (F32, i64) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (F32, F32) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i32;
    type Modulo = F64;
    type LeastSuper = F32;
}

impl ResultTypeOfBinary for (F32, F64) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (F64, u8) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (F64, u16) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (F64, u32) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (F64, u64) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (F64, i8) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (F64, i16) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (F64, i32) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (F64, i64) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (F64, F32) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfBinary for (F64, F64) {
    type AddMul = F64;
    type Minus = F64;
    type IntDiv = i64;
    type Modulo = F64;
    type LeastSuper = F64;
}

impl ResultTypeOfUnary for u8 {
    type Negate = i16;
    type Sum = u64;

    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.checked_add(rhs)
    }

    fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.checked_sub(rhs)
    }

    fn checked_mul(self, rhs: Self) -> Option<Self> {
        self.checked_mul(rhs)
    }

    fn checked_div(self, rhs: Self) -> Option<Self> {
        self.checked_div(rhs)
    }

    fn checked_rem(self, rhs: Self) -> Option<Self> {
        self.checked_rem(rhs)
    }
}

impl ResultTypeOfUnary for u16 {
    type Negate = i32;
    type Sum = u64;

    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.checked_add(rhs)
    }

    fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.checked_sub(rhs)
    }

    fn checked_mul(self, rhs: Self) -> Option<Self> {
        self.checked_mul(rhs)
    }

    fn checked_div(self, rhs: Self) -> Option<Self> {
        self.checked_div(rhs)
    }

    fn checked_rem(self, rhs: Self) -> Option<Self> {
        self.checked_rem(rhs)
    }
}

impl ResultTypeOfUnary for u32 {
    type Negate = i64;
    type Sum = u64;

    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.checked_add(rhs)
    }

    fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.checked_sub(rhs)
    }

    fn checked_mul(self, rhs: Self) -> Option<Self> {
        self.checked_mul(rhs)
    }

    fn checked_div(self, rhs: Self) -> Option<Self> {
        self.checked_div(rhs)
    }

    fn checked_rem(self, rhs: Self) -> Option<Self> {
        self.checked_rem(rhs)
    }
}

impl ResultTypeOfUnary for u64 {
    type Negate = i64;
    type Sum = u64;

    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.checked_add(rhs)
    }

    fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.checked_sub(rhs)
    }

    fn checked_mul(self, rhs: Self) -> Option<Self> {
        self.checked_mul(rhs)
    }

    fn checked_div(self, rhs: Self) -> Option<Self> {
        self.checked_div(rhs)
    }

    fn checked_rem(self, rhs: Self) -> Option<Self> {
        self.checked_rem(rhs)
    }
}

impl ResultTypeOfUnary for i8 {
    type Negate = i8;
    type Sum = i64;

    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.checked_add(rhs)
    }

    fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.checked_sub(rhs)
    }

    fn checked_mul(self, rhs: Self) -> Option<Self> {
        self.checked_mul(rhs)
    }

    fn checked_div(self, rhs: Self) -> Option<Self> {
        self.checked_div(rhs)
    }

    fn checked_rem(self, rhs: Self) -> Option<Self> {
        self.checked_rem(rhs)
    }
}

impl ResultTypeOfUnary for i16 {
    type Negate = i16;
    type Sum = i64;

    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.checked_add(rhs)
    }

    fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.checked_sub(rhs)
    }

    fn checked_mul(self, rhs: Self) -> Option<Self> {
        self.checked_mul(rhs)
    }

    fn checked_div(self, rhs: Self) -> Option<Self> {
        self.checked_div(rhs)
    }

    fn checked_rem(self, rhs: Self) -> Option<Self> {
        self.checked_rem(rhs)
    }
}

impl ResultTypeOfUnary for i32 {
    type Negate = i32;
    type Sum = i64;

    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.checked_add(rhs)
    }

    fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.checked_sub(rhs)
    }

    fn checked_mul(self, rhs: Self) -> Option<Self> {
        self.checked_mul(rhs)
    }

    fn checked_div(self, rhs: Self) -> Option<Self> {
        self.checked_div(rhs)
    }

    fn checked_rem(self, rhs: Self) -> Option<Self> {
        self.checked_rem(rhs)
    }
}

impl ResultTypeOfUnary for i64 {
    type Negate = i64;
    type Sum = i64;

    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.checked_add(rhs)
    }

    fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.checked_sub(rhs)
    }

    fn checked_mul(self, rhs: Self) -> Option<Self> {
        self.checked_mul(rhs)
    }

    fn checked_div(self, rhs: Self) -> Option<Self> {
        self.checked_div(rhs)
    }

    fn checked_rem(self, rhs: Self) -> Option<Self> {
        self.checked_rem(rhs)
    }
}

impl ResultTypeOfUnary for F32 {
    type Negate = F32;
    type Sum = F64;

    fn checked_add(self, rhs: Self) -> Option<Self> {
        Some(self + rhs)
    }

    fn checked_sub(self, rhs: Self) -> Option<Self> {
        Some(self - rhs)
    }

    fn checked_mul(self, rhs: Self) -> Option<Self> {
        Some(self * rhs)
    }

    fn checked_div(self, rhs: Self) -> Option<Self> {
        Some(self / rhs)
    }

    fn checked_rem(self, rhs: Self) -> Option<Self> {
        Some(self % rhs)
    }
}

impl ResultTypeOfUnary for F64 {
    type Negate = F64;
    type Sum = F64;

    fn checked_add(self, rhs: Self) -> Option<Self> {
        Some(self + rhs)
    }

    fn checked_sub(self, rhs: Self) -> Option<Self> {
        Some(self - rhs)
    }

    fn checked_mul(self, rhs: Self) -> Option<Self> {
        Some(self * rhs)
    }

    fn checked_div(self, rhs: Self) -> Option<Self> {
        Some(self / rhs)
    }

    fn checked_rem(self, rhs: Self) -> Option<Self> {
        Some(self % rhs)
    }
}
