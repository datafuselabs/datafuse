module.exports = async ({ github, context, core }) => {
  const knownEvents = ["schedule", "workflow_dispatch", "release"];
  if (!knownEvents.includes(context.eventName)) {
    core.setFailed(`Triggerd by unknown event: ${context.eventName}`);
    return;
  }

  const { STABLE, TAG } = process.env;

  // trigger by release event
  if (context.ref.startsWith("refs/tags/")) {
    let tag = context.ref.replace("refs/tags/", "");
    core.setOutput("tag", tag);
    core.setOutput("sha", context.sha);
    core.info(`Tag event triggered by ${tag}.`);
    return;
  }

  // trigger by schedule or workflow_dispatch event
  if (STABLE == "true") {
    if (TAG) {
      // trigger stable release by workflow_dispatch with a tag
      let result = /v(\d+)\.(\d+)\.(\d+)-nightly/g.exec(TAG);
      if (!result) {
        core.setFailed(`The tag ${TAG} to stablize is invalid, ignoring`);
        return;
      }
      let major = result[1];
      let minor = result[2];
      let patch = result[3];
      let stableTag = `v${major}.${minor}.${patch}`;
      core.setOutput("tag", stableTag);
      let ref = await github.rest.git.getRef({
        owner: context.repo.owner,
        repo: context.repo.repo,
        ref: `tags/${TAG}`,
      });
      core.setOutput("sha", ref.data.object.sha);
      core.info(
        `Stable release ${stableTag} from ${TAG} (${ref.data.object.sha})`
      );
    } else {
      core.setFailed("Stable release must be triggered with a nightly tag");
    }
  } else {
    core.setOutput("sha", context.sha);
    if (TAG) {
      core.setOutput("tag", TAG);
      core.info(`Release create manually with tag ${TAG} (${context.sha})`);
    } else {
      let releases = await github.rest.repos.listReleases({
        owner: context.repo.owner,
        repo: context.repo.repo,
        per_page: 10,
      });
      let tag = releases.data.filter(
        (r) => r.tag_name.startsWith("v") && r.tag_name.endsWith("-nightly")
      )[0];
      if (!tag) {
        core.setFailed(`No previous nightly release found, ignoring`);
        return;
      }
      let lastTag = tag.tag_name;
      let result = /v(\d+)\.(\d+)\.(\d+)/g.exec(lastTag);
      if (!result) {
        core.setFailed(`The previous tag ${lastTag} is invalid, ignoring`);
        return;
      }
      let major = result[1];
      let minor = result[2];
      let patch = (parseInt(result[3]) + 1).toString();
      let nextTag = `v${major}.${minor}.${patch}-nightly`;
      core.setOutput("tag", nextTag);
      core.info(`Nightly release ${nextTag} from ${lastTag} (${context.sha})`);
    }
  }
};
