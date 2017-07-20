extern crate argparse;
extern crate rustyline;

use rustyline::Editor;
use rustyline::error::ReadlineError;

fn main() {
    println!("Welcome to SoupDB!");
    let mut prompt = Editor::<()>::new();
    loop {
        let line = prompt.readline("soup>> ");
        match line {
            Ok(line) => {
                // TODO
                prompt.add_history_entry(&line);
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
}
