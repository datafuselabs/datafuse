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

//! Kv API with `kvapi::Key` type key and protobuf encoded value.

use std::future::Future;

use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_kvapi::kvapi::NonEmptyItem;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::InvalidReply;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaNetworkError;
use databend_common_meta_types::SeqV;
use databend_common_proto_conv::FromToProto;
use databend_common_proto_conv::Incompatible;
use futures::future::FutureExt;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use PbApiReadError::KvApiError;

/// An error occurs when decoding protobuf encoded value.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[error("PbDecodeError: {0}")]
pub enum PbDecodeError {
    DecodeError(#[from] prost::DecodeError),
    Incompatible(#[from] Incompatible),
}

/// An error occurs when found an unexpected None value.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[error("NoneValue: unexpected None value of key: '{key}'")]
pub struct NoneValue {
    key: String,
}

impl NoneValue {
    pub fn new(key: impl ToString) -> Self {
        NoneValue {
            key: key.to_string(),
        }
    }
}

/// An error occurs when reading protobuf encoded value from kv store.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[error("PbApiReadError: {0}")]
pub enum PbApiReadError<E> {
    DecodeError(#[from] prost::DecodeError),
    Incompatible(#[from] Incompatible),
    KeyError(#[from] kvapi::KeyError),
    NoneValue(#[from] NoneValue),
    /// Error returned from KVApi.
    KvApiError(E),
}

impl<E> From<PbDecodeError> for PbApiReadError<E> {
    fn from(e: PbDecodeError) -> Self {
        match e {
            PbDecodeError::DecodeError(e) => PbApiReadError::DecodeError(e),
            PbDecodeError::Incompatible(e) => PbApiReadError::Incompatible(e),
        }
    }
}

impl From<PbApiReadError<MetaError>> for MetaError {
    /// For KVApi that returns MetaError, convert protobuf related error to MetaError directly.
    ///
    /// Because MetaError contains network protocol level error variant.
    /// If there is a decoding error, consider it as network level error.
    fn from(value: PbApiReadError<MetaError>) -> Self {
        match value {
            PbApiReadError::DecodeError(e) => {
                let inv = InvalidReply::new("", &e);
                let net_err = MetaNetworkError::InvalidReply(inv);
                MetaError::NetworkError(net_err)
            }
            PbApiReadError::Incompatible(e) => {
                let inv = InvalidReply::new("", &e);
                let net_err = MetaNetworkError::InvalidReply(inv);
                MetaError::NetworkError(net_err)
            }
            PbApiReadError::KeyError(e) => {
                let inv = InvalidReply::new("", &e);
                let net_err = MetaNetworkError::InvalidReply(inv);
                MetaError::NetworkError(net_err)
            }
            PbApiReadError::NoneValue(e) => {
                let inv = InvalidReply::new("", &e);
                let net_err = MetaNetworkError::InvalidReply(inv);
                MetaError::NetworkError(net_err)
            }
            KvApiError(e) => e,
        }
    }
}

/// This trait provides a way to access a kv store with `kvapi::Key` type key and protobuf encoded value.
pub trait KVPbApi: KVApi {
    /// Get protobuf encoded value by kvapi::Key.
    ///
    /// The key will be converted to string and the returned value is decoded by `FromToProto`.
    /// It returns the same error as `KVApi::Error`,
    /// thus it requires KVApi::Error can describe a decoding error, i.e., `impl From<PbApiReadError>`.
    fn get_pb<K>(
        &self,
        key: &K,
    ) -> impl Future<Output = Result<Option<SeqV<K::ValueType>>, Self::Error>> + Send
    where
        K: kvapi::Key,
        K::ValueType: FromToProto,
        Self::Error: From<PbApiReadError<Self::Error>>,
    {
        self.get_pb_low(key).map(|r| r.map_err(Self::Error::from))
    }

    /// Same as `get_pb` but returns [`PbApiReadError`]. No require of `From<PbApiReadError>` for `Self::Error`.
    fn get_pb_low<K>(
        &self,
        key: &K,
    ) -> impl Future<Output = Result<Option<SeqV<K::ValueType>>, PbApiReadError<Self::Error>>> + Send
    where
        K: kvapi::Key,
        K::ValueType: FromToProto,
    {
        let key = key.to_string_key();
        async move {
            let raw_seqv = self.get_kv(&key).await.map_err(KvApiError)?;
            let v = raw_seqv.map(decode_seqv::<K::ValueType>).transpose()?;
            Ok(v)
        }
    }

    /// List protobuf encoded values by prefix and returns a stream.
    ///
    /// The returned value is decoded by `FromToProto`.
    /// It returns the same error as `KVApi::Error`,
    /// thus it requires KVApi::Error can describe a decoding error, i.e., `impl From<PbApiReadError>`.
    fn list_pb<K>(
        &self,
        prefix: &DirName<K>,
    ) -> impl Future<
        Output = Result<BoxStream<'static, Result<NonEmptyItem<K>, Self::Error>>, Self::Error>,
    > + Send
    where
        K: kvapi::Key + 'static,
        K::ValueType: FromToProto,
        Self::Error: From<PbApiReadError<Self::Error>>,
    {
        self.list_pb_low(prefix).map(|r| match r {
            Ok(strm) => Ok(strm.map_err(Self::Error::from).boxed()),
            Err(e) => Err(Self::Error::from(e)),
        })
    }

    /// Same as `list_pb` but returns [`PbApiReadError`]. No require of `From<PbApiReadError>` for `Self::Error`.
    fn list_pb_low<K>(
        &self,
        prefix: &DirName<K>,
    ) -> impl Future<
        Output = Result<
            BoxStream<'static, Result<NonEmptyItem<K>, PbApiReadError<Self::Error>>>,
            PbApiReadError<Self::Error>,
        >,
    > + Send
    where
        K: kvapi::Key + 'static,
        K::ValueType: FromToProto,
    {
        let prefix = prefix.to_string_key();
        async move {
            let strm = self.list_kv(&prefix).await.map_err(KvApiError)?;
            let strm = strm.map(decode_non_empty_item::<K, Self::Error>);
            Ok(strm.boxed())
        }
    }
}

impl<T> KVPbApi for T where T: KVApi + ?Sized {}

/// Decode key and protobuf encoded value from `StreamItem`.
///
/// It requires K to be static because it is used in a static stream map()
fn decode_non_empty_item<K, E>(
    r: Result<StreamItem, E>,
) -> Result<NonEmptyItem<K>, PbApiReadError<E>>
where
    K: kvapi::Key + 'static,
    K::ValueType: FromToProto,
{
    match r {
        Ok(item) => {
            let k = K::from_str_key(&item.key)?;

            let raw = item.value.ok_or_else(|| NoneValue::new(item.key))?;
            let v = decode_seqv::<K::ValueType>(SeqV::from(raw))?;

            Ok(NonEmptyItem::new(k, v))
        }
        Err(e) => Err(KvApiError(e)),
    }
}

/// Deserialize SeqV<Vec<u8>> into SeqV<T>, with FromToProto.
fn decode_seqv<T>(seqv: SeqV) -> Result<SeqV<T>, PbDecodeError>
where T: FromToProto {
    let buf = &seqv.data;
    let p: T::PB = prost::Message::decode(buf.as_ref())?;
    let v: T = FromToProto::from_pb(p)?;

    Ok(SeqV::with_meta(seqv.seq, seqv.meta, v))
}

#[cfg(test)]
mod tests {
    use crate::kv_pb_api::PbDecodeError;

    #[test]
    fn test_error_message() {
        let e = PbDecodeError::DecodeError(prost::DecodeError::new("decode error"));
        assert_eq!(
            "PbDecodeError: failed to decode Protobuf message: decode error",
            e.to_string()
        );
    }
}
