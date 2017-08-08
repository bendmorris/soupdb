use std::fs::create_dir_all;
use std::path::PathBuf;

pub struct Config {
    pub data_dir: PathBuf,
}

impl Config {
    pub fn new() -> Config {
         let new_config = Config {data_dir: ["/var", "soupdb"].iter().collect()};
         new_config.create_directories();
         new_config
    }

    pub fn create_directories(&self) {
        create_dir_all(&self.data_dir);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config() {
        // make sure constructor works
        let config = Config::new();
        assert!(config.data_dir.to_str().unwrap().len() > 0);
    }
}
