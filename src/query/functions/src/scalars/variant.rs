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

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::iter::once;
use std::sync::Arc;

use bstr::ByteSlice;
use chrono::Datelike;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_arrow::arrow::temporal_conversions::EPOCH_DAYS_FROM_CE;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::date::string_to_date;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::nullable::NullableColumnBuilder;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::number::*;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::timestamp::string_to_timestamp;
use databend_common_expression::types::variant::cast_scalar_to_variant;
use databend_common_expression::types::variant::cast_scalars_to_variants;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::GenericType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::ALL_NUMERICS_TYPES;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::vectorize_with_builder_3_arg;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_expression::ValueRef;
use jsonb::RawJsonb;
use jsonb::build_array;
use jsonb::build_object;
use jsonb::jsonpath::parse_json_path;
use jsonb::jsonpath::JsonPath;
use jsonb::keypath::parse_key_paths;
use jsonb::parse_value;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_aliases("json_object_keys", &["object_keys"]);
    registry.register_aliases("to_string", &["json_to_string"]);

    registry.register_passthrough_nullable_1_arg::<VariantType, VariantType, _, _>(
        "parse_json",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, VariantType>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            // Variant value may be an invalid JSON, convert them to string and then parse.
            let val = RawJsonb(s).to_string();
            match parse_value(val.as_bytes()) {
                Ok(value) => {
                    value.write_to_vec(&mut output.data);
                }
                Err(err) => {
                    if ctx.func_ctx.disable_variant_check {
                        output.put_str("");
                    } else {
                        ctx.set_error(output.len(), err.to_string());
                    }
                }
            }
            output.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, VariantType, _, _>(
        "parse_json",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, VariantType>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            match parse_value(s.as_bytes()) {
                Ok(value) => {
                    value.write_to_vec(&mut output.data);
                }
                Err(err) => {
                    if ctx.func_ctx.disable_variant_check {
                        output.put_str("");
                    } else {
                        ctx.set_error(output.len(), err.to_string());
                    }
                }
            }
            output.commit_row();
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, VariantType, _, _>(
        "try_parse_json",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<VariantType>>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            // Variant value may be an invalid JSON, convert them to string and then parse.
            let val = RawJsonb(s).to_string();
            match parse_value(val.as_bytes()) {
                Ok(value) => {
                    output.validity.push(true);
                    value.write_to_vec(&mut output.builder.data);
                    output.builder.commit_row();
                }
                Err(_) => output.push_null(),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<StringType, VariantType, _, _>(
        "try_parse_json",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<StringType, NullableType<VariantType>>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match parse_value(s.as_bytes()) {
                Ok(value) => {
                    output.validity.push(true);
                    value.write_to_vec(&mut output.builder.data);
                    output.builder.commit_row();
                }
                Err(_) => output.push_null(),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, StringType, _, _>(
        "check_json",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<StringType>>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            // Variant value may be an invalid JSON, convert them to string and then check.
            let val = RawJsonb(s).to_string();
            match parse_value(val.as_bytes()) {
                Ok(_) => output.push_null(),
                Err(e) => output.push(&e.to_string()),
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<StringType, StringType, _, _>(
        "check_json",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<StringType, NullableType<StringType>>(|s, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match parse_value(s.as_bytes()) {
                Ok(_) => output.push_null(),
                Err(e) => output.push(&e.to_string()),
            }
        }),
    );

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<UInt32Type>, _, _>(
        "length",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<NullableType<VariantType>, NullableType<UInt32Type>>(|val, _| {
            val.and_then(|v| {
                match RawJsonb(v).array_length() {
                    Ok(len) => len.map(|len| len as u32),
                    Err(_) => {
                        parse_value(v).ok().and_then(|v| v.array_length().map(|len| len as u32))
                    }
                }
            })
        }),
    );

    registry.register_1_arg_core::<NullableType<VariantType>, NullableType<VariantType>, _, _>(
        "json_object_keys",
        |_, _| FunctionDomain::Full,
        vectorize_1_arg::<NullableType<VariantType>, NullableType<VariantType>>(|val, _| {
            val.and_then(|v| {
                match RawJsonb(v).object_keys() {
                    Ok(obj_keys) => obj_keys,
                    Err(_) => {
                        parse_value(v).ok().and_then(|v| v.object_keys().map(|obj_keys| {
                            let mut buf = Vec::new();
                            obj_keys.write_to_vec(&mut buf);
                            buf
                        }))
                    }
                }
            })
        }),
    );

    // get_by_keypath
    // get_by_keypath_string


    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "get",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |val, name, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match RawJsonb(val).get_by_name(name, false) {
                    Ok(Some(v)) => {
                        output.push(&v);
                    }
                    Ok(None) => {
                        output.push_null();
                    }
                    Err(_) => {
                        if let Ok(val) = parse_value(val) {
                            // todo
                            if let Some(res_val) = val.get_by_name_ignore_case(name) {
                                let mut buf = Vec::new();
                                res_val.write_to_vec(&mut buf);
                                output.push(&buf);
                                return;
                            }
                        }
                        output.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, Int64Type, VariantType, _, _>(
        "get",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, Int64Type, NullableType<VariantType>>(
            |val, idx, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                if idx < 0 || idx > i32::MAX.into() {
                    output.push_null();
                } else {
                    match RawJsonb(val).get_by_index(idx as usize) {
                        Ok(Some(v)) => {
                            output.push(&v);
                        }
                        Ok(None) => {
                            output.push_null();
                        }
                        Err(_) => {
                            if let Ok(val) = parse_value(val) {
                                // todo
                                let name = "a";
                                if let Some(res_val) = val.get_by_name_ignore_case(&name) {
                                    let mut buf = Vec::new();
                                    res_val.write_to_vec(&mut buf);
                                    output.push(&buf);
                                    return;
                                }
                            }
                            output.push_null();
                        }
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, StringType, VariantType, _, _>(
        "get_ignore_case",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<VariantType>>(
            |val, name, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match RawJsonb(val).get_by_name(name, true) {
                    Ok(Some(v)) => output.push(&v),
                    Ok(None) => output.push_null(),
                    Err(_) => {
                        if let Ok(val) = parse_value(val) {
                            // todo
                            if let Some(res_val) = val.get_by_name_ignore_case(&name) {
                                let mut buf = Vec::new();
                                res_val.write_to_vec(&mut buf);
                                output.push(&buf);
                                return;
                            }
                        }
                        output.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, StringType, StringType, _, _>(
        "get_string",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, StringType, NullableType<StringType>>(
            |val, name, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match RawJsonb(val).get_by_name(name, false) {
                    Ok(Some(v)) => {
                        let json_str = RawJsonb(v).to_string();
                        output.push(&json_str);
                    }
                    Ok(None) => output.push_null(),
                    Err(_) => {
                        if let Ok(val) = parse_value(val) {
                            // todo
                            if let Some(res_val) = val.get_by_name_ignore_case(&name) {
                                let json_str = format!("{}", res_val);
                                output.push(&json_str);
                                return;
                            }
                        }
                        output.push_null();
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, Int64Type, StringType, _, _>(
        "get_string",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, Int64Type, NullableType<StringType>>(
            |val, idx, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                if idx < 0 || idx > i32::MAX.into() {
                    output.push_null();
                } else {
                    match RawJsonb(val).get_by_index(idx as usize) {
                        Ok(Some(v)) => {
                            let json_str = RawJsonb(v).to_string();
                            output.push(&json_str);
                        }
                        Ok(None) => {
                            output.push_null();
                        }
                        Err(_) => {
                            if let Ok(val) = parse_value(val) {
                                // todo
                                let name = "a";
                                if let Some(res_val) = val.get_by_name_ignore_case(&name) {
                                    let json_str = format!("{}", res_val);
                                    output.push(&json_str);
                                    return;
                                }
                            }
                            output.push_null();
                        }
                    }
                }
            },
        ),
    );

    // json_path_query_array
    // json_path_query_first
    // json_path_match
    // json_path_exists
    // get_path
    // json_extract_path_text


    registry.register_combine_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "as_boolean",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<BooleanType>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match RawJsonb(v).as_bool() {
                Ok(Some(res)) => output.push(res),
                Ok(None) => output.push_null(),
                Err(_) => {
                    match parse_value(v).ok().and_then(|v| v.as_bool()) {
                        Some(res) => output.push(res),
                        None => output.push_null(),
                    }
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, Int64Type, _, _>(
        "as_integer",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<Int64Type>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match RawJsonb(v).as_i64() {
                Ok(Some(res)) => output.push(res),
                Ok(None) => output.push_null(),
                Err(_) => {
                    match parse_value(v).ok().and_then(|v| v.as_i64()) {
                        Some(res) => output.push(res),
                        None => output.push_null(),
                    }
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, Float64Type, _, _>(
        "as_float",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<Float64Type>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match RawJsonb(v).as_f64() {
                Ok(Some(res)) => output.push(res.into()),
                Ok(None) => output.push_null(),
                Err(_) => {
                    match parse_value(v).ok().and_then(|v| v.as_f64()) {
                        Some(res) => output.push(res.into()),
                        None => output.push_null(),
                    }
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, StringType, _, _>(
        "as_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<StringType>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match RawJsonb(v).as_str() {
                Ok(Some(res)) => output.push(&res),
                Ok(None) => output.push_null(),
                Err(_) => {
                    if let Ok(val) = parse_value(v) {
                        if let Some(res) = val.as_str() {
                            output.push(res);
                            return;
                        }
                    }
                    output.push_null();
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, VariantType, _, _>(
        "as_array",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<VariantType>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match RawJsonb(v).is_array() {
                Ok(true) => output.push(v.as_bytes()),
                Ok(false) => output.push_null(),
                Err(_) => {
                    if let Ok(val) = parse_value(v) {
                        if val.is_array() {
                            let mut buf = Vec::new();
                            val.write_to_vec(&mut buf);
                            output.push(&buf);
                            return;
                        }
                    }
                    output.push_null();
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, VariantType, _, _>(
        "as_object",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<VariantType>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match RawJsonb(v).is_object() {
                Ok(true) => output.push(v.as_bytes()),
                Ok(false) => output.push_null(),
                Err(_) => {
                    if let Ok(val) = parse_value(v) {
                        if val.is_object() {
                            let mut buf = Vec::new();
                            val.write_to_vec(&mut buf);
                            output.push(&buf);
                            return;
                        }
                    }
                    output.push_null();
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_null_value",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match RawJsonb(v).is_null() {
                Ok(res) => output.push(res),
                Err(_) => {
                    match parse_value(v).ok().and_then(|v| Some(v.is_null())) {
                        Some(res) => output.push(res),
                        None => output.push(false),
                    }
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_boolean",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match RawJsonb(v).is_boolean() {
                Ok(res) => output.push(res),
                Err(_) => {
                    match parse_value(v).ok().and_then(|v| Some(v.is_boolean())) {
                        Some(res) => output.push(res),
                        None => output.push(false),
                    }
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_integer",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match RawJsonb(v).is_i64() {
                Ok(res) => output.push(res),
                Err(_) => {
                    match parse_value(v).ok().and_then(|v| Some(v.is_i64())) {
                        Some(res) => output.push(res),
                        None => output.push(false),
                    }
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_float",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match RawJsonb(v).is_f64() {
                Ok(res) => output.push(res),
                Err(_) => {
                    match parse_value(v).ok().and_then(|v| Some(v.is_f64())) {
                        Some(res) => output.push(res),
                        None => output.push(false),
                    }
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match RawJsonb(v).is_string() {
                Ok(res) => output.push(res),
                Err(_) => {
                    match parse_value(v).ok().and_then(|v| Some(v.is_string())) {
                        Some(res) => output.push(res),
                        None => output.push(false),
                    }
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_array",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match RawJsonb(v).is_array() {
                Ok(res) => output.push(res),
                Err(_) => {
                    match parse_value(v).ok().and_then(|v| Some(v.is_array())) {
                        Some(res) => output.push(res),
                        None => output.push(false),
                    }
                }
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "is_object",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match RawJsonb(v).is_object() {
                Ok(res) => output.push(res),
                Err(_) => {
                    match parse_value(v).ok().and_then(|v| Some(v.is_object())) {
                        Some(res) => output.push(res),
                        None => output.push(false),
                    }
                }
            }
        }),
    );

    registry.register_function_factory("to_variant", |_, args_type| {
        if args_type.len() != 1 {
            return None;
        }
        let return_type = if args_type[0].is_nullable_or_null() {
            DataType::Nullable(Box::new(DataType::Variant))
        } else {
            DataType::Variant
        };

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "to_variant".to_string(),
                args_type: vec![DataType::Generic(0)],
                return_type,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, args_domain| match &args_domain[0] {
                    Domain::Nullable(nullable_domain) => {
                        FunctionDomain::Domain(Domain::Nullable(NullableDomain {
                            has_null: nullable_domain.has_null,
                            value: Some(Box::new(Domain::Undefined)),
                        }))
                    }
                    _ => FunctionDomain::Domain(Domain::Undefined),
                }),
                eval: Box::new(|args, ctx| match &args[0] {
                    ValueRef::Scalar(scalar) => match scalar {
                        ScalarRef::Null => Value::Scalar(Scalar::Null),
                        _ => {
                            let mut buf = Vec::new();
                            cast_scalar_to_variant(scalar.clone(), ctx.func_ctx.tz, &mut buf);
                            Value::Scalar(Scalar::Variant(buf))
                        }
                    },
                    ValueRef::Column(col) => {
                        let validity = match col {
                            Column::Null { len } => Some(Bitmap::new_constant(false, *len)),
                            Column::Nullable(box ref nullable_column) => {
                                Some(nullable_column.validity.clone())
                            }
                            _ => None,
                        };
                        let new_col = cast_scalars_to_variants(col.iter(), ctx.func_ctx.tz);
                        if let Some(validity) = validity {
                            Value::Column(NullableColumn::new_column(
                                Column::Variant(new_col),
                                validity,
                            ))
                        } else {
                            Value::Column(Column::Variant(new_col))
                        }
                    }
                }),
            },
        }))
    });

    registry.register_combine_nullable_1_arg::<GenericType<0>, VariantType, _, _>(
        "try_to_variant",
        |_, domain| {
            let has_null = match domain {
                Domain::Nullable(nullable_domain) => nullable_domain.has_null,
                _ => false,
            };
            FunctionDomain::Domain(NullableDomain {
                has_null,
                value: Some(Box::new(())),
            })
        },
        |val, ctx| match val {
            ValueRef::Scalar(scalar) => match scalar {
                ScalarRef::Null => Value::Scalar(None),
                _ => {
                    let mut buf = Vec::new();
                    cast_scalar_to_variant(scalar, ctx.func_ctx.tz, &mut buf);
                    Value::Scalar(Some(buf))
                }
            },
            ValueRef::Column(col) => {
                let validity = match col {
                    Column::Null { len } => Bitmap::new_constant(false, len),
                    Column::Nullable(box ref nullable_column) => nullable_column.validity.clone(),
                    _ => Bitmap::new_constant(true, col.len()),
                };
                let new_col = cast_scalars_to_variants(col.iter(), ctx.func_ctx.tz);
                Value::Column(NullableColumn::new(new_col, validity))
            }
        },
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "to_boolean",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, BooleanType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(false);
                    return;
                }
            }
            match cast_to_bool(v) {
                Ok(res) => output.push(res),
                Err(_) => {
                    ctx.set_error(output.len(), "unable to cast to type `BOOLEAN`");
                    output.push(false);
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, BooleanType, _, _>(
        "try_to_boolean",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<BooleanType>>(
            |v, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match cast_to_bool(v) {
                    Ok(res) => output.push(res),
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, StringType, _, _>(
        "to_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, StringType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            let json_str = cast_to_string(v);
            output.put_str(&json_str);
            output.commit_row();
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, StringType, _, _>(
        "try_to_string",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<StringType>>(
            |v, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                let json_str = cast_to_string(v);
                output.push(&json_str);
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, DateType, _, _>(
        "to_date",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, DateType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(0);
                    return;
                }
            }
            match cast_to_str(v) {
                Ok(val) => match string_to_date(
                    val.as_bytes(),
                    ctx.func_ctx.tz.tz,
                    ctx.func_ctx.enable_dst_hour_fix,
                ) {
                    Ok(d) => output.push(d.num_days_from_ce() - EPOCH_DAYS_FROM_CE),
                    Err(e) => {
                        ctx.set_error(
                            output.len(),
                            format!("unable to cast to type `DATE`. {}", e),
                        );
                        output.push(0);
                    }
                },
                Err(_) => {
                    ctx.set_error(output.len(), "unable to cast to type `DATE`");
                    output.push(0);
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, DateType, _, _>(
        "try_to_date",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<DateType>>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push_null();
                    return;
                }
            }
            match cast_to_str(v) {
                Ok(val) => match string_to_date(
                    val.as_bytes(),
                    ctx.func_ctx.tz.tz,
                    ctx.func_ctx.enable_dst_hour_fix,
                ) {
                    Ok(d) => output.push(d.num_days_from_ce() - EPOCH_DAYS_FROM_CE),
                    Err(_) => output.push_null(),
                },
                Err(_) => output.push_null(),
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, TimestampType, _, _>(
        "to_timestamp",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, TimestampType>(|v, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.push(0);
                    return;
                }
            }
            match cast_to_str(v) {
                Ok(val) => match string_to_timestamp(
                    val.as_bytes(),
                    ctx.func_ctx.tz.tz,
                    ctx.func_ctx.enable_dst_hour_fix,
                ) {
                    Ok(ts) => output.push(ts.timestamp_micros()),
                    Err(e) => {
                        ctx.set_error(
                            output.len(),
                            format!("unable to cast to type `TIMESTAMP`. {}", e),
                        );
                        output.push(0);
                    }
                },
                Err(_) => {
                    ctx.set_error(output.len(), "unable to cast to type `TIMESTAMP`");
                    output.push(0);
                }
            }
        }),
    );

    registry.register_combine_nullable_1_arg::<VariantType, TimestampType, _, _>(
        "try_to_timestamp",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<TimestampType>>(
            |v, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match cast_to_str(v) {
                    Ok(val) => match string_to_timestamp(
                        val.as_bytes(),
                        ctx.func_ctx.tz.tz,
                        ctx.func_ctx.enable_dst_hour_fix,
                    ) {
                        Ok(ts) => output.push(ts.timestamp_micros()),
                        Err(_) => {
                            output.push_null();
                        }
                    },
                    Err(_) => {
                        output.push_null();
                    }
                }
            },
        ),
    );

    for dest_type in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match dest_type {
            NumberDataType::NUM_TYPE => {
                let name = format!("to_{dest_type}").to_lowercase();
                registry
                    .register_passthrough_nullable_1_arg::<VariantType, NumberType<NUM_TYPE>, _, _>(
                        &name,
                        |_, _| FunctionDomain::MayThrow,
                        vectorize_with_builder_1_arg::<VariantType, NumberType<NUM_TYPE>>(
                            move |v, output, ctx| {
                                if let Some(validity) = &ctx.validity {
                                    if !validity.get_bit(output.len()) {
                                        output.push(NUM_TYPE::default());
                                        return;
                                    }
                                }
                                type Native = <NUM_TYPE as Number>::Native;

                                let value: Option<Native> = if dest_type.is_float() {
                                    cast_to_f64(v).ok().and_then(num_traits::cast::cast)
                                } else if dest_type.is_signed() {
                                    cast_to_i64(v).ok().and_then(num_traits::cast::cast)
                                } else {
                                    cast_to_u64(v).ok().and_then(num_traits::cast::cast)
                                };
                                match value {
                                    Some(value) => output.push(value.into()),
                                    None => {
                                        ctx.set_error(
                                            output.len(),
                                            format!("unable to cast to type {dest_type}",),
                                        );
                                        output.push(NUM_TYPE::default());
                                    }
                                }
                            },
                        ),
                    );

                let name = format!("try_to_{dest_type}").to_lowercase();
                registry
                    .register_combine_nullable_1_arg::<VariantType, NumberType<NUM_TYPE>, _, _>(
                        &name,
                        |_, _| FunctionDomain::Full,
                        vectorize_with_builder_1_arg::<
                            VariantType,
                            NullableType<NumberType<NUM_TYPE>>,
                        >(move |v, output, ctx| {
                            if let Some(validity) = &ctx.validity {
                                if !validity.get_bit(output.len()) {
                                    output.push_null();
                                    return;
                                }
                            }
                            if dest_type.is_float() {
                                if let Ok(value) = cast_to_f64(v) {
                                    if let Some(new_value) = num_traits::cast::cast(value) {
                                        output.push(new_value);
                                    } else {
                                        output.push_null();
                                    }
                                } else {
                                    output.push_null();
                                }
                            } else if dest_type.is_signed() {
                                if let Ok(value) = cast_to_i64(v) {
                                    if let Some(new_value) = num_traits::cast::cast(value) {
                                        output.push(new_value);
                                    } else {
                                        output.push_null();
                                    }
                                } else {
                                    output.push_null();
                                }
                            } else {
                                if let Ok(value) = cast_to_u64(v) {
                                    if let Some(new_value) = num_traits::cast::cast(value) {
                                        output.push(new_value);
                                    } else {
                                        output.push_null();
                                    }
                                } else {
                                    output.push_null();
                                }
                            }
                        }),
                    );
            }
        });
    }

    registry.register_passthrough_nullable_1_arg::<VariantType, StringType, _, _>(
        "json_pretty",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, StringType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            let s = RawJsonb(val).to_pretty_string();
            output.put_str(&s);
            output.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg(
        "json_strip_nulls",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, VariantType>(|val, output, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output.len()) {
                    output.commit_row();
                    return;
                }
            }
            if let Err(err) = RawJsonb(val).strip_nulls(&mut output.data) {
                ctx.set_error(output.len(), err.to_string());
            };
            output.commit_row();
        }),
    );





}



// Extract string for string type, other types convert to JSON string.
fn cast_to_string(v: &[u8]) -> String {
    let raw_jsonb = RawJsonb(v);
    match raw_jsonb.to_str() {
        Ok(v) => v,
        Err(_) => raw_jsonb.to_string(),
    }
}

fn cast_to_str(v: &[u8]) -> Result<String, jsonb::Error> {
    match RawJsonb(v).to_str() {
        Ok(val) => {
            Ok(val)
        }
        Err(err) => {
            if err.to_string() == "InvalidJsonb" {
                if let Ok(val) = parse_value(v) {
                    return val.to_str();
                }
            }
            Err(err)
        }
    }
}

fn cast_to_bool(v: &[u8]) -> Result<bool, jsonb::Error> {
    match RawJsonb(v).to_bool() {
        Ok(val) => {
            Ok(val)
        }
        Err(err) => {
            if err.to_string() == "InvalidJsonb" {
                if let Ok(val) = parse_value(v) {
                    return val.to_bool();
                }
            }
            Err(err)
        }
    }
}


fn cast_to_i64(v: &[u8]) -> Result<i64, jsonb::Error> {
    match RawJsonb(v).to_i64() {
        Ok(val) => {
            Ok(val)
        }
        Err(err) => {
            if err.to_string() == "InvalidJsonb" {
                if let Ok(val) = parse_value(v) {
                    return val.to_i64();
                }
            }
            Err(err)
        }
    }
}

fn cast_to_u64(v: &[u8]) -> Result<u64, jsonb::Error> {
    match RawJsonb(v).to_u64() {
        Ok(val) => {
            Ok(val)
        }
        Err(err) => {
            if err.to_string() == "InvalidJsonb" {
                if let Ok(val) = parse_value(v) {
                    return val.to_u64();
                }
            }
            Err(err)
        }
    }
}

fn cast_to_f64(v: &[u8]) -> Result<f64, jsonb::Error> {
    match RawJsonb(v).to_f64() {
        Ok(val) => {
            Ok(val)
        }
        Err(err) => {
            if err.to_string() == "InvalidJsonb" {
                if let Ok(val) = parse_value(v) {
                    return val.to_f64();
                }
            }
            Err(err)
        }
    }
}
