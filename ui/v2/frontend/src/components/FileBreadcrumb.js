import {Breadcrumbs, Link, Typography} from "@mui/material";

function FileBreadcrumbs({snapshotid, file}) {
    return (
        <>
            <Breadcrumbs>
                {/*{file.path.map((path, index) => (*/}
                <Link href={'https://localhost:1234'}>bla</Link>
                <Link href={'https://localhost:1234'}>bla</Link>
                <Link href={'https://localhost:1234'}>bla</Link>
                {/*))}*/}
            </Breadcrumbs>
        </>
    );
}

export default FileBreadcrumbs;