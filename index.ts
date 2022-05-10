import { pickBy, uniqBy } from 'lodash';
import { Octokit } from 'octokit';
import { format } from '@fast-csv/format';
import { resolve } from 'path';
import { createWriteStream } from 'fs';

function createStream(type: 'users' | 'repositories') {
  const stream = format({ headers: true, quote: true });
  stream.pipe(createWriteStream(resolve(__dirname, `${type}.csv`)));
  return stream;
}

(async () => {
  if (!process.env.BASE_URL) throw new Error('BASE_URL must be set on environment variables!');

  const octokit = new Octokit({ baseUrl: process.env.BASE_URL });

  const repositoriesIds = new Set<number>();
  const usersIds = new Set<number>();

  let maxStargazers = null;
  let hasNextPage = true;

  const userStream = createStream('users');
  const reposStream = createStream('repositories');

  while (hasNextPage) {
    const iterator = octokit.paginate.iterator(octokit.rest.search.repos, {
      q: `awesome is:featured sort:stars-desc ${maxStargazers ? `stars:1..${maxStargazers}` : ''}`,
      per_page: 100,
    });

    for await (const { data } of iterator) {
      const uDiff = data
        .map((repo) => repo.owner)
        .filter((user) => user && !usersIds.has(user.id))
        .map((user) => pickBy(user, (_, key) => !key.endsWith('url')));

      const rDiff = data
        .filter((d) => !repositoriesIds.has(d.id))
        .map((repo) => ({
          ...repo,
          owner: repo.owner?.id,
          license: repo.license?.key,
          topics: repo.topics?.join(', '),
        }))
        .map((repo) => pickBy(repo, (_, key) => !key.endsWith('url') && key !== 'permissions'));

      uniqBy(rDiff, 'id').forEach((repo) => {
        if (!repo) return;
        repositoriesIds.add(repo.id as any);
        reposStream.write(repo);
      });

      uniqBy(uDiff, 'id').forEach((user) => {
        if (!user) return;
        usersIds.add(user.id as any);
        userStream.write(user);
      });

      hasNextPage = rDiff.length === 0;
      maxStargazers = Math.min(
        ...rDiff.map((repo) => (repo.stargazers_count as number) || Number.MAX_SAFE_INTEGER),
      );

      // eslint-disable-next-line no-console
      console.log({
        repos: repositoriesIds.size,
        users: usersIds.size,
        hasNextPage,
        maxStargazers,
      });
    }
  }

  userStream.end();
  reposStream.end();
})();
