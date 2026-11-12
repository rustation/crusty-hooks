pub use config::NO_CONFIG_FILE_FOUND;
use std::collections::HashMap;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{Instrument, instrument};

mod config;
mod git;
mod init_directory;

mod hooks;
pub use config::ConfigFile;
pub use hooks::{HOOK_NAMES, NO_CONFIG_FILE_FOUND_ERROR_CODE};

pub fn init<F, G, H>(
    run_command: F,
    write_file: G,
    file_exists: H,
    hook_file_skip_list: Vec<&str>,
) -> Result<(), String>
where
    F: Fn(
        &str,
        Option<&str>,
        bool,
        Option<&HashMap<String, String>>,
    ) -> Result<Option<String>, Option<String>>,
    G: Fn(&str, &str, bool) -> Result<(), String>,
    H: Fn(&str) -> Result<bool, ()>,
{
    init_directory::init_directory(
        &run_command,
        &write_file,
        &file_exists,
        None,
        hook_file_skip_list,
    )
}

pub async fn run<F, G, H>(
    run_command: F,
    file_exists: G,
    read_file: H,
    hook_name: &str,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Fn(
        &str,
        Option<&str>,
        bool,
        Option<&HashMap<String, String>>,
    ) -> Result<Option<String>, Option<String>>,
    G: Fn(&str) -> Result<bool, ()>,
    H: Fn(&str) -> Result<String, ()>,
{
    let root_directory_path = match git::get_root_directory_path(&run_command, None) {
        Ok(Some(path)) => path,
        _ => {
            return Err(String::from("Failure determining git repo root directory"))?;
        }
    };

    let config_file_contents =
        config::get_config_file_contents(read_file, file_exists, &root_directory_path).map_err(
            |e| {
                if e == config::NO_CONFIG_FILE_FOUND {
                    e
                } else {
                    String::from("Failed to parse config file")
                }
            },
        )?;

    let mut config_file = ConfigFile::try_from_str(&config_file_contents)?;

    let Some(hooks) = config_file.hooks.remove(hook_name) else {
        return Ok(());
    };

    let mut handle = tokio::task::JoinSet::new();

    for xs in hooks {
        let root_directory_path = root_directory_path.clone();

        handle.spawn(async move {
            for x in xs {
                run_task(x, root_directory_path.clone()).await?;
            }

            Ok::<_, std::io::Error>(())
        });
    }

    while let Some(x) = handle.join_next().await {
        x??;
    }

    Ok(())
}

#[instrument(skip(root_directory_path), err)]
async fn run_task(x: String, root_directory_path: String) -> Result<(), std::io::Error> {
    tracing::info!("Running {x}");

    let (envs, cmd) = parse_env_and_command(&x);

    let mut args = cmd.into_iter();

    let cmd = args.next().unwrap();

    let mut cmd = tokio::process::Command::new(cmd);

    cmd.args(args)
        .current_dir(&root_directory_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if !envs.is_empty() {
        cmd.envs(&envs);
    }

    let mut child = cmd.spawn()?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("failed to capture child stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| std::io::Error::other("failed to capture child stderr"))?;

    let span = tracing::Span::current();

    let stdout_task = tokio::spawn(
        async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::info!("{line}");
            }
        }
        .instrument(span.clone()),
    );

    let stderr_task = tokio::spawn(
        async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::warn!("{line}");
            }
        }
        .instrument(span),
    );

    let status = child.wait().await?;
    let _ = tokio::join!(stdout_task, stderr_task);

    if !status.success() {
        return Err(std::io::Error::other(format!(
            "Command `{x}` failed with exit code {:?}",
            status.code()
        )));
    }

    tracing::info!("Finished {x}");

    Ok(())
}

fn parse_env_and_command(input: &str) -> (HashMap<String, String>, Vec<String>) {
    let mut env_vars = HashMap::new();
    let mut command_args = Vec::new();

    if let Some(parts) = shlex::split(input) {
        let mut found_command = false;

        for part in parts {
            // Check if it's an env var assignment (KEY=VALUE pattern)
            if !found_command
                && part.contains('=')
                && let Some((key, value)) = part.split_once('=')
            {
                // Validate that the key looks like a valid env var name
                if key.chars().all(|c| c.is_alphanumeric() || c == '_')
                    && !key.is_empty()
                    && !key.chars().next().unwrap().is_numeric()
                {
                    env_vars.insert(key.to_string(), value.to_string());
                    continue;
                }
            }

            // Once we hit a non-env-var pattern, treat rest as command
            found_command = true;
            command_args.push(part);
        }
    }

    (env_vars, command_args)
}

#[cfg(test)]
mod tests;
