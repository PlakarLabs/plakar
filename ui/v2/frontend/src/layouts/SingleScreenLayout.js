import DefaultLayout from "./DefaultLayout";
import {Stack} from "@mui/material";
import ConfigShield from "../components/ConfigShield";

function SingleScreenLayout({children, conf}) {
    return (
        <ConfigShield>
            <DefaultLayout conf={conf}>
                <Stack sx={{p: 2}}>
                    <Stack sx={{backgroundColor: 'white', p: 2, borderRadius: 2}}>
                        {children}
                    </Stack>
                </Stack>
            </DefaultLayout>
        </ConfigShield>
    )
};

export default SingleScreenLayout;
