#!/usr/bin/env php
<?php

$tags = shell_exec('git tag -l');
$tags = explode("\n", $tags);
$tags = array_filter($tags);
usort($tags, 'version_compare');

$bad_tags = [
];

$printed = [];

foreach ($tags as $tag) {
    if (in_array($tag, $bad_tags)) {
        continue;
    }

    $version = preg_replace('/(v\d+\.\d+)\.\d+$/', '$1', $tag);

    $content = shell_exec("git tag -l --format='%(subject)' {$tag}");
    $content = preg_replace('/ *- /', "\n- ", $content);
    $content = trim($content);

    if (empty($content)) {
        $content = shell_exec("git rev-list --format='%s' --max-count=1 {$tag}");
        $content = preg_replace('/^commit.*/', '', $content);
        $content = trim($content);
    }

    if (!in_array($version, $printed)) {
        $printed[] = $version;

        echo "\n\n";
        echo "### {$version}\n\n";
        echo "{$content}\n";
    }
    else {
        echo "- {$content}\n";
    }
}
