import {useEffect} from "react";
import {shallowEqual, useSelector} from "react-redux";
import {selectApiUrl} from "../state/Root";
import {useNavigate} from "react-router-dom";
import {CONFIG_ROUTE} from "../utils/Routes";

// This component will redirect the user to the configuration page if the API URL is not set
const ConfigShield = ({children}) => {
    const navigate = useNavigate()
    const apiUrl = useSelector(selectApiUrl, shallowEqual);

    useEffect(() => {
        if (!apiUrl) {
            navigate(CONFIG_ROUTE);
        }
        return () => {

        }
    }, [apiUrl, navigate]);

    return (
        <div>
            {children}
        </div>
    )

}

export default ConfigShield;