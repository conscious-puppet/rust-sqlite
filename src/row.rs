use std::{fmt, str::FromStr};

use crate::statement::PrepareStatementErr;

pub const ID_SIZE: usize = size_of::<u32>();
pub const USERNAME_SIZE: usize = 32;
pub const EMAIL_SIZE: usize = 255;
pub const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

pub struct Row {
    id: u32,
    username: [u8; USERNAME_SIZE],
    email: [u8; EMAIL_SIZE],
}

impl Row {
    pub fn new<'a>(id: &str, username: &str, email: &str) -> Result<Self, PrepareStatementErr<'a>> {
        let id = id
            .parse::<u32>()
            .map_err(|_| PrepareStatementErr::InvalidID)?;

        let username_bytes = username.as_bytes();
        let mut username = [0; USERNAME_SIZE];
        username[..username_bytes.len()].copy_from_slice(username_bytes);

        let email_bytes = email.as_bytes();
        let mut email = [0; EMAIL_SIZE];
        email[..email_bytes.len()].copy_from_slice(email_bytes);

        Ok(Self {
            id,
            username,
            email,
        })
    }

    pub fn username(&self) -> String {
        String::from_utf8_lossy(&self.username)
            .trim_end_matches(char::from(0))
            .to_string()
    }

    pub fn email(&self) -> String {
        String::from_utf8_lossy(&self.email)
            .trim_end_matches(char::from(0))
            .to_string()
    }

    pub fn serialize(&self) -> [u8; ROW_SIZE] {
        let mut row: [u8; ROW_SIZE] = [0; ROW_SIZE];
        let id_bytes = self.id.to_le_bytes();

        let start = 0;
        let end = ID_SIZE;
        row[start..end].copy_from_slice(&id_bytes);

        let start = end;
        let end = start + USERNAME_SIZE;
        row[start..end].copy_from_slice(&self.username);

        let start = end;
        let end = start + EMAIL_SIZE;
        row[start..end].copy_from_slice(&self.email);
        row
    }

    pub fn deserialize(row: [u8; ROW_SIZE]) -> Self {
        let start = 0;
        let end = ID_SIZE;
        let mut id_bytes = [0; ID_SIZE];
        id_bytes.copy_from_slice(&row[start..end]);
        let id = u32::from_le_bytes(id_bytes);

        let start = end;
        let end = start + USERNAME_SIZE;
        let mut username = [0; USERNAME_SIZE];
        username.copy_from_slice(&row[start..end]);

        let start = end;
        let end = start + EMAIL_SIZE;
        let mut email = [0; EMAIL_SIZE];
        email.copy_from_slice(&row[start..end]);

        Self {
            id,
            username,
            email,
        }
    }
}

impl FromStr for Row {
    type Err = PrepareStatementErr<'static>;

    fn from_str(row: &str) -> Result<Self, Self::Err> {
        let columns: Vec<&str> = row.split_whitespace().collect();
        match columns[..] {
            [id, username, email] => {
                if username.bytes().len() > USERNAME_SIZE {
                    return Err(PrepareStatementErr::StringTooLong);
                }

                if email.bytes().len() > EMAIL_SIZE {
                    return Err(PrepareStatementErr::StringTooLong);
                }

                Self::new(id, username, email)
            }
            _ => Err(PrepareStatementErr::SyntaxError),
        }
    }
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}, {}, {})", self.id, self.username(), self.email())
    }
}
