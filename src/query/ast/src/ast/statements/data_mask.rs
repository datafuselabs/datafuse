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

use std::fmt::Display;
use std::fmt::Formatter;

use crate::ast::Expr;
use crate::ast::TypeName;

#[derive(Debug, Clone, PartialEq)]
pub struct DataMaskArg {
    pub arg_name: String,
    pub arg_type: TypeName,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DataMaskPolicy {
    pub args: Vec<DataMaskArg>,
    pub return_type: TypeName,
    pub body: Expr,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateDatamaskPolicyStmt {
    pub if_not_exists: bool,
    pub name: String,
    pub policy: DataMaskPolicy,
}

impl Display for CreateDatamaskPolicyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE MASKING POLICY ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{} AS (", self.name)?;
        let mut flag = false;
        for arg in &self.policy.args {
            if flag {
                write!(f, ",")?;
            }
            flag = true;
            write!(f, "{} {}", arg.arg_name, arg.arg_type)?;
        }
        write!(
            f,
            ") RETURNS {} -> {}",
            self.policy.return_type, self.policy.body
        )?;
        if let Some(comment) = &self.policy.comment {
            write!(f, " COMMENT = '{}'", comment)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropDatamaskPolicyStmt {
    pub if_exists: bool,
    pub name: String,
}

impl Display for DropDatamaskPolicyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP MASKING POLICY ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.name)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DescDatamaskPolicyStmt {
    pub name: String,
}

impl Display for DescDatamaskPolicyStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESCRIBE MASKING POLICY {}", self.name)?;

        Ok(())
    }
}
